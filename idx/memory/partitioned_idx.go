package memory

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Implements the the "MetricIndex" interface
type PartitionedMemoryIdx struct {
	Partition map[int32]*UnpartitionedMemoryIdx
}

func NewPartitionedMemoryIdx() *PartitionedMemoryIdx {
	idx := &PartitionedMemoryIdx{
		Partition: make(map[int32]*UnpartitionedMemoryIdx),
	}
	partitions := cluster.Manager.GetPartitions()
	log.Infof("PartitionedMemoryIdx: initializing with partitions: %v", partitions)
	for _, p := range partitions {
		idx.Partition[p] = NewUnpartitionedMemoryIdx()
	}
	return idx
}

// Init initializes the index at startup and
// blocks until the index is ready for use.
func (p *PartitionedMemoryIdx) Init() error {
	for _, m := range p.Partition {
		err := m.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

// Stop shuts down the index.
func (p *PartitionedMemoryIdx) Stop() {
	for _, m := range p.Partition {
		m.Stop()
	}
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (p *PartitionedMemoryIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	return p.Partition[partition].Update(point, partition)
}

// AddOrUpdate makes sure a metric is known in the index,
// and should be called for every received metric.
func (p *PartitionedMemoryIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	return p.Partition[partition].AddOrUpdate(mkey, data, partition)
}

// UpdateArchive updates the archive information
func (p *PartitionedMemoryIdx) UpdateArchiveLastSave(id schema.MKey, partition int32, lastSave uint32) {
	p.Partition[partition].UpdateArchiveLastSave(id, partition, lastSave)
}

// Get returns the archive for the requested id.
func (p *PartitionedMemoryIdx) Get(key schema.MKey) (idx.Archive, bool) {
	g, _ := errgroup.WithContext(context.Background())
	resultChan := make(chan *idx.Archive, len(p.Partition))
	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			if a, ok := m.Get(key); ok {
				resultChan <- &a
			}
			return nil
		})
	}

	var err atomic.Value
	go func() {
		if e := g.Wait(); e != nil {
			err.Store(e)
		}
		close(resultChan)
	}()

	result, ok := <-resultChan
	if !ok {
		e := err.Load()
		if e != nil {
			log.Errorf("memory-idx: failed to get Archive with key %v. %s", key, e)
		}
		return idx.Archive{}, false
	}

	return *result, true
}

// GetPath returns the archives under the given path.
func (p *PartitionedMemoryIdx) GetPath(orgId uint32, path string) []idx.Archive {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			result[pos] = m.GetPath(orgId, path)
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to getPath: orgId=%d path=%s. %s", orgId, path, err)
		return nil
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response
}

// Delete deletes items from the index
// If the pattern matches a branch node, then
// all leaf nodes on that branch are deleted. So if the pattern is
// "*", all items in the index are deleted.
// It returns a copy of all of the Archives deleted.
func (p *PartitionedMemoryIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			deleted, err := m.Delete(orgId, pattern)
			if err != nil {
				return err
			}
			result[pos] = deleted
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to Delete: orgId=%d pattern=%s. %s", orgId, pattern, err)
		return nil, err
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response, nil
}

// Find searches the index for matching nodes.
// * orgId describes the org to search in (public data in orgIdPublic is automatically included)
// * pattern is handled like graphite does. see https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
// * from is a unix timestamp. series not updated since then are excluded.
func (p *PartitionedMemoryIdx) Find(orgId uint32, pattern string, from int64) ([]idx.Node, error) {
	g, _ := errgroup.WithContext(context.Background())
	resultChan := make(chan []idx.Node)
	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			found, err := m.Find(orgId, pattern, from)
			if err != nil {
				return err
			}
			resultChan <- found
			return nil
		})
	}

	// consume from the ResultChan and merge the results, de-duping branches
	byPath := make(map[string]*idx.Node)
	done := make(chan struct{})
	go func() {
		for found := range resultChan {
			for _, node := range found {
				n, ok := byPath[node.Path]
				if !ok {
					byPath[node.Path] = &node
				} else {
					if node.HasChildren {
						n.HasChildren = true
					}
					if node.Leaf {
						n.Defs = append(n.Defs, node.Defs...)
						n.Leaf = true
					}
				}
			}
		}
		close(done)
	}()
	if err := g.Wait(); err != nil {
		close(resultChan)
		log.Errorf("memory-idx: failed to Find: orgId=%d pattern=%s from=%d. %s", orgId, pattern, from, err)
		return nil, err
	}
	close(resultChan)
	<-done
	response := make([]idx.Node, 0, len(byPath))
	for _, node := range byPath {
		response = append(response, *node)
	}
	return response, nil
}

// List returns all Archives for the passed OrgId and the public orgId
func (p *PartitionedMemoryIdx) List(orgId uint32) []idx.Archive {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found := m.List(orgId)
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to List: orgId=%d. %s", orgId, err)
		return nil
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response
}

// Prune deletes all metrics that haven't been seen since the given timestamp.
// It returns all Archives deleted and any error encountered.
func (p *PartitionedMemoryIdx) Prune(oldest time.Time) ([]idx.Archive, error) {
	// Prune each partition sequentially.
	result := []idx.Archive{}
	for _, m := range p.Partition {
		found, err := m.Prune(oldest)
		if err != nil {
			return result, err
		}
		result = append(result, found...)
	}
	return result, nil
}

// FindByTag takes a list of expressions in the format key<operator>value.
// The allowed operators are: =, !=, =~, !=~.
// It returns a slice of Node structs that match the given conditions, the
// conditions are logically AND-ed.
// If the third argument is > 0 then the results will be filtered and only those
// where the LastUpdate time is >= from will be returned as results.
// The returned results are not deduplicated and in certain cases it is possible
// that duplicate entries will be returned.
func (p *PartitionedMemoryIdx) FindByTag(orgId uint32, expressions []string, from int64) ([]idx.Node, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Node, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.FindByTag(orgId, expressions, from)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to FindByTag: orgId=%d expressions=%v from=%d. %s", orgId, expressions, from, err)
		return nil, err
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Node, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response, nil
}

// Tags returns a list of all tag keys associated with the metrics of a given
// organization. The return values are filtered by the regex in the second parameter.
// If the third parameter is >0 then only metrics will be accounted of which the
// LastUpdate time is >= the given value.
func (p *PartitionedMemoryIdx) Tags(orgId uint32, filter string, from int64) ([]string, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.Tags(orgId, filter, from)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to get Tags: orgId=%d filter=%v from=%d. %s", orgId, filter, from, err)
		return nil, err
	}

	// merge our results into the unique set of tags
	merged := map[string]struct{}{}
	for _, tags := range result {
		for _, t := range tags {
			merged[t] = struct{}{}
		}
	}
	if len(merged) == 0 {
		return nil, nil
	}
	response := make([]string, 0, len(merged))
	for tag := range merged {
		response = append(response, tag)
	}

	return response, nil
}

// FindTags generates a list of possible tags that could complete a
// given prefix. It also accepts additional tag conditions to further narrow
// down the result set in the format of graphite's tag queries
func (p *PartitionedMemoryIdx) FindTags(orgId uint32, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.FindTags(orgId, prefix, expressions, from, limit)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to get Tags: orgId=%d prefix=%s expressions=%v from=%d limit=%d. %s", orgId, prefix, expressions, from, limit, err)
		return nil, err
	}

	// merge our results into the unique set of tags
	merged := map[string]struct{}{}
	for _, tags := range result {
		for _, t := range tags {
			merged[t] = struct{}{}
		}
	}
	response := make([]string, 0, len(merged))
	for tag := range merged {
		response = append(response, tag)
	}
	sort.Strings(response)
	if uint(len(response)) > limit {
		return response[:limit], nil
	}
	return response, nil
}

// FindTagValues generates a list of possible values that could
// complete a given value prefix. It requires a tag to be specified and only values
// of the given tag will be returned. It also accepts additional conditions to
// further narrow down the result set in the format of graphite's tag queries
func (p *PartitionedMemoryIdx) FindTagValues(orgId uint32, tag string, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]string, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.FindTagValues(orgId, tag, prefix, expressions, from, limit)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to FindTagValues: orgId=%d tag=%s prefix=%s expressions=%v from=%d limit=%d. %s", orgId, tag, prefix, expressions, from, limit, err)
		return nil, err
	}

	// merge our results into the unique set of tags
	merged := map[string]struct{}{}
	for _, tags := range result {
		for _, t := range tags {
			merged[t] = struct{}{}
		}
	}
	response := make([]string, 0, len(merged))
	for tag := range merged {
		response = append(response, tag)
	}

	return response, nil
}

// TagDetails returns a list of all values associated with a given tag key in the
// given org. The occurrences of each value is counted and the count is referred to by
// the metric names in the returned map.
// If the third parameter is not "" it will be used as a regular expression to filter
// the values before accounting for them.
// If the fourth parameter is > 0 then only those metrics of which the LastUpdate
// time is >= the from timestamp will be included.
func (p *PartitionedMemoryIdx) TagDetails(orgId uint32, key string, filter string, from int64) (map[string]uint64, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([]map[string]uint64, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.TagDetails(orgId, key, filter, from)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to get TagDetails: orgId=%d key=%s filter=%s from=%d. %s", orgId, key, filter, from, err)
		return nil, err
	}

	// merge our results into the unique set of tags
	merged := map[string]uint64{}
	for _, tagCounts := range result {
		for tag, count := range tagCounts {
			merged[tag] = merged[tag] + count
		}
	}

	return merged, nil
}

// DeleteTagged deletes the specified series from the tag index and also the
// DefById index.
func (p *PartitionedMemoryIdx) DeleteTagged(orgId uint32, paths []string) ([]idx.Archive, error) {
	g, _ := errgroup.WithContext(context.Background())
	result := make([][]idx.Archive, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found, err := m.DeleteTagged(orgId, paths)
			if err != nil {
				return err
			}
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to DeleteTagged: orgId=%d paths=%v. %s", orgId, paths, err)
		return nil, err
	}

	// get our total count, so we can allocate our response in one go.
	items := 0
	for _, r := range result {
		items += len(r)
	}
	response := make([]idx.Archive, 0, items)
	for _, r := range result {
		response = append(response, r...)
	}
	return response, nil
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (p *PartitionedMemoryIdx) LoadPartition(partition int32, defs []schema.MetricDefinition) int {
	return p.Partition[partition].Load(defs)
}

func (p *PartitionedMemoryIdx) add(archive *idx.Archive) {
	p.Partition[archive.Partition].add(archive)
}

func (p *PartitionedMemoryIdx) idsByTagQuery(orgId uint32, query TagQuery) IdSet {
	g, _ := errgroup.WithContext(context.Background())
	result := make([]IdSet, len(p.Partition))
	var i int
	for _, m := range p.Partition {
		pos, m := i, m
		g.Go(func() error {
			found := m.idsByTagQuery(orgId, query)
			result[pos] = found
			return nil
		})
		i++
	}
	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to get idsByTagQuery: orgId=%d query=%v. %s", orgId, query, err)
		return nil
	}

	response := make(IdSet)
	for _, ids := range result {
		for mk := range ids {
			response[mk] = struct{}{}
		}
	}
	return response
}

func (p *PartitionedMemoryIdx) MetaTagRecordList(orgId uint32) []idx.MetaTagRecord {
	for _, m := range p.Partition {
		// all partitions should have all meta records
		return m.MetaTagRecordList(orgId)
	}
	return nil
}

func (p *PartitionedMemoryIdx) MetaTagRecordUpsert(orgId uint32, rawRecord idx.MetaTagRecord) (idx.MetaTagRecord, bool, error) {
	g, _ := errgroup.WithContext(context.Background())

	var i int
	var record idx.MetaTagRecord
	var created bool
	for _, m := range p.Partition {
		m := m
		g.Go(func() error {
			var err error
			if i == 0 {
				record, created, err = m.MetaTagRecordUpsert(orgId, rawRecord)
			} else {
				_, _, err = m.MetaTagRecordUpsert(orgId, rawRecord)
			}

			return err
		})
		i++
	}

	if err := g.Wait(); err != nil {
		log.Errorf("memory-idx: failed to upsert meta tag record in at least one partition: %s", err)
		return record, created, err
	}

	return record, created, nil
}
