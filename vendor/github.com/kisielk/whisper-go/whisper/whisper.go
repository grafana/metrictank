/*

Package whisper implements an interface to the whisper database format used by the Graphite project (https://github.com/graphite-project/)

*/
package whisper

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Metadata holds metadata that's common to an entire whisper database
type Metadata struct {
	// Aggregation method used. See the Aggregation* constants.
	AggregationMethod AggregationMethod

	// The maximum retention period.
	MaxRetention uint32

	// The minimum percentage of known values required to aggregate.
	XFilesFactor float32

	// The number of archives in the database.
	ArchiveCount uint32
}

// ArchiveInfo holds metadata about a single archive within a whisper database
type ArchiveInfo struct {
	Offset          uint32 // The byte offset of the archive within the database
	SecondsPerPoint uint32 // The number of seconds of elapsed time represented by a data point
	Points          uint32 // The number of data points
}

// NewArchiveInfo returns a new ArchiveInfo with a zero offset.
func NewArchiveInfo(secondsPerPoint, points uint32) ArchiveInfo {
	return ArchiveInfo{SecondsPerPoint: secondsPerPoint, Points: points}
}

// Retention is the retention period of the archive in seconds.
func (a ArchiveInfo) Retention() uint32 {
	return a.SecondsPerPoint * a.Points
}

// Size is the size of the archive in bytes.
func (a ArchiveInfo) Size() uint32 {
	return a.Points * pointSize
}

// end is the byte offset of the last point in the archive.
func (a ArchiveInfo) end() uint32 {
	return a.Offset + a.Size()
}

// The AggregationMethod type describes how values are aggregated from one Whisper archive to another.
type AggregationMethod uint32

func (m AggregationMethod) String() (s string) {
	switch m {
	case AggregationAverage:
		s = "average"
	case AggregationSum:
		s = "sum"
	case AggregationLast:
		s = "last"
	case AggregationMin:
		s = "min"
	case AggregationMax:
		s = "max"
	default:
		s = "unknown"
	}
	return
}

// Valid aggregation methods
const (
	AggregationUnknown AggregationMethod = 0 // Unknown aggregation method
	AggregationAverage AggregationMethod = 1 // Aggregate using averaging
	AggregationSum     AggregationMethod = 2 // Aggregate using sum
	AggregationLast    AggregationMethod = 3 // Aggregate using the last value
	AggregationMax     AggregationMethod = 4 // Aggregate using the maximum value
	AggregationMin     AggregationMethod = 5 // Aggregate using the minimum value
)

const (
	DefaultXFilesFactor      = 0.5
	DefaultAggregationMethod = AggregationAverage
)

// Header contains all the metadata about a whisper database.
type Header struct {
	Metadata Metadata      // General metadata about the database
	Archives []ArchiveInfo // Information about each of the archives in the database, in order of precision
}

// Point is a single datum stored in a whisper database.
type Point struct {
	Timestamp uint32  // Timestamp in seconds past the epoch
	Value     float64 // Data point value
}

// NewPoint constructs a new Point at time t with value v.
func NewPoint(t time.Time, v float64) Point {
	return Point{uint32(t.Unix()), v}
}

// Time returns the time for the Point p
func (p Point) Time() time.Time {
	return time.Unix(int64(p.Timestamp), 0)
}

// Interval repsents a time interval with a step.
type Interval struct {
	FromTimestamp  uint32 // Start of the interval in seconds since the epoch
	UntilTimestamp uint32 // End of the interval in seconds since the epoch
	Step           uint32 // Step size in seconds
}

// From returns the interval's FromTimestamp as a time.Time
func (i Interval) From() time.Time {
	return time.Unix(int64(i.FromTimestamp), 0)
}

// Until returns the interval's UntilTimestamp as a time.Time
func (i Interval) Until() time.Time {
	return time.Unix(int64(i.UntilTimestamp), 0)
}

// Duration returns the interval length as a time.Duration
func (i Interval) Duration() time.Duration {
	return i.Until().Sub(i.From())
}

// Whisper represents a handle to a whisper database.
type Whisper struct {
	Header Header
	file   io.ReadWriteSeeker
}

// type for sorting a list of ArchiveInfo by the SecondsPerPoint field
type bySecondsPerPoint []ArchiveInfo

// sort.Interface
func (a bySecondsPerPoint) Len() int           { return len(a) }
func (a bySecondsPerPoint) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a bySecondsPerPoint) Less(i, j int) bool { return a[i].SecondsPerPoint < a[j].SecondsPerPoint }

// a list of points
type archive []Point

// sort.Interface
func (a archive) Len() int           { return len(a) }
func (a archive) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archive) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

// a type for reverse sorting of archives
type reverseArchive struct{ archive }

// sort.Interface
func (r reverseArchive) Less(i, j int) bool { return r.archive.Less(j, i) }

var (
	pointSize       = uint32(binary.Size(Point{}))
	metadataSize    = uint32(binary.Size(Metadata{}))
	archiveInfoSize = uint32(binary.Size(ArchiveInfo{}))
)

// a regular expression matching a precision string such as 120y
var precisionRegexp = regexp.MustCompile("^(\\d+)([smhdwy]?)")

// readHeader attempts to read the header of a Whisper database.
func readHeader(buf io.ReadSeeker) (header Header, err error) {
	_, err = buf.Seek(0, 0)
	if err != nil {
		return
	}

	var metadata Metadata
	err = binary.Read(buf, binary.BigEndian, &metadata)
	if err != nil {
		return
	}
	header.Metadata = metadata

	archives := make([]ArchiveInfo, metadata.ArchiveCount)
	for i := uint32(0); i < metadata.ArchiveCount; i++ {
		err = binary.Read(buf, binary.BigEndian, &archives[i])
		if err != nil {
			return
		}
	}
	header.Archives = archives

	return
}

var (
	ErrNoArchives         = errors.New("archive list must contain at least one archive.")
	ErrDuplicateArchive   = errors.New("no archive may be a duplicate of another.")
	ErrUnevenPrecision    = errors.New("higher precision archives must evenly divide in to lower precision.")
	ErrLowRetention       = errors.New("lower precision archives must cover a larger time interval than higher precision.")
	ErrInsufficientPoints = errors.New("archive has insufficient points to aggregate to a lower precision")
)

/*

validateArchiveList validates a list of ArchiveInfos.

The list must:

1. Have at least one ArchiveInfo

2. No archive may be a duplicate of another.

3. Higher precision archives' precision must evenly divide all lower precision archives' precision.

4. Lower precision archives must cover larger time intervals than higher precision archives.

5. Each archive must have at least enough points to consolidate to the next archive

*/
func validateArchiveList(archives []ArchiveInfo) error {
	sort.Sort(bySecondsPerPoint(archives))

	// 1.
	if len(archives) == 0 {
		return ErrNoArchives
	}

	for i := 0; i < len(archives)-1; i++ {
		archive := archives[i]
		nextArchive := archives[i+1]

		// 2.
		if archive.SecondsPerPoint == nextArchive.SecondsPerPoint {
			return ErrDuplicateArchive
		}

		// 3.
		if nextArchive.SecondsPerPoint%archive.SecondsPerPoint != 0 {
			return ErrUnevenPrecision
		}

		// 4.
		nextRetention := nextArchive.Retention()
		retention := archive.Retention()
		if !(nextRetention > retention) {
			return ErrLowRetention
		}

		// 5.
		if !(archive.Points >= (nextArchive.SecondsPerPoint / archive.SecondsPerPoint)) {
			return ErrInsufficientPoints
		}

	}
	return nil

}

// CreateOptions sets the options used to create a new archive.
type CreateOptions struct {
	// The XFiles factor to use. DefaultXFilesFactor if not set.
	XFilesFactor float32

	// The archive aggregation method to use. DefaultAggregationMethod if not set.
	AggregationMethod AggregationMethod

	// If true, allocate a sparse archive.
	Sparse bool
}

func DefaultCreateOptions() CreateOptions {
	return CreateOptions{DefaultXFilesFactor, DefaultAggregationMethod, false}
}
// headerSize calculates the size of a header with n archives
func headerSize(n int) uint32 {
	return metadataSize + (archiveInfoSize * uint32(n))
}

// Create attempts to create a new database at the given filepath.
func Create(path string, archives []ArchiveInfo, options CreateOptions) (*Whisper, error) {
	if err := validateArchiveList(archives); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}

	oldest := uint32(0)
	for _, archive := range archives {
		age := archive.SecondsPerPoint * archive.Points
		if age > oldest {
			oldest = age
		}
	}

	metadata := Metadata{
		AggregationMethod: options.AggregationMethod,
		XFilesFactor:      options.XFilesFactor,
		ArchiveCount:      uint32(len(archives)),
		MaxRetention:      oldest,
	}
	if err := binary.Write(file, binary.BigEndian, metadata); err != nil {
		return nil, err
	}

	hSize := headerSize(len(archives))
	archiveOffsetPointer := hSize

	for _, archive := range archives {
		archive.Offset = archiveOffsetPointer
		if err := binary.Write(file, binary.BigEndian, archive); err != nil {
			return nil, err
		}
		archiveOffsetPointer += archive.Points * pointSize
	}

	if options.Sparse {
		file.Seek(int64(archiveOffsetPointer-hSize-1), 0)
		file.Write([]byte{0})
	} else {
		remaining := archiveOffsetPointer - hSize
		chunkSize := uint32(16384)
		buf := make([]byte, chunkSize)
		for remaining > chunkSize {
			file.Write(buf)
			remaining -= chunkSize
		}
		file.Write(buf[:remaining])
	}

	return OpenWhisper(file)
}

// OpenWhisper opens an existing Whisper database from the given ReadWriteSeeker.
func OpenWhisper(f io.ReadWriteSeeker) (*Whisper, error) {
	header, err := readHeader(f)
	if err != nil {
		return nil, err
	}
	return &Whisper{Header: header, file: f}, nil
}

// Open opens an existing whisper database.
// It returns an error if the file does not exist or is not a valid whisper archive.
func Open(path string) (*Whisper, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return OpenWhisper(file)
}

// Close closes a whisper database.
func (w *Whisper) Close() error {
	if closer, ok := w.file.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (w *Whisper) DumpArchive(n int) ([]Point, error) {
	if n >= len(w.Header.Archives) {
		return nil, fmt.Errorf("database contains only %d archives", len(w.Header.Archives))
	} else if n < 0 {
		return nil, fmt.Errorf("archive index must be greater than 0")
	}

	info := w.Header.Archives[n]
	points := make([]Point, int(info.Points))
	err := w.readPoints(info.Offset, points)
	return points, err
}

// Update writes a single datapoint to the whisper database
func (w *Whisper) Update(point Point) error {
	now := uint32(time.Now().Unix())
	diff := now - point.Timestamp
	if !((diff < w.Header.Metadata.MaxRetention) && diff >= 0) {
		return errors.New("point is older than the maximum retention period")
	}

	// Find the higher-precision archive that covers the timestamp
	var lowerArchives []ArchiveInfo
	var currentArchive ArchiveInfo
	for i, ca := range w.Header.Archives {
		if ca.Retention() > diff {
			lowerArchives = w.Header.Archives[i+1:]
			currentArchive = ca
			break
		}
	}

	// Normalize the point's timestamp to the current archive's precision and write the point
	point.Timestamp = quantize(point.Timestamp, currentArchive.SecondsPerPoint)
	if err := w.writeArchive(currentArchive, point); err != nil {
		return err
	}

	// Propagate data down to all the lower resolution archives
	higherArchive := currentArchive
	for _, lowerArchive := range lowerArchives {
		result, err := w.propagate(point.Timestamp, higherArchive, lowerArchive)
		if err != nil {
			return err
		}
		if !result {
			break
		}
		higherArchive = lowerArchive
	}

	return nil
}

// UpdateMany write a slice of datapoints to the whisper database. The points
// do not have to be unique or sorted. If two points cover the same time
// interval the last point encountered will be retained.
func (w *Whisper) UpdateMany(points []Point) error {
	now := uint32(time.Now().Unix())

	// TODO: Sort points first

	archiveIndex := 0
	currentArchive := &w.Header.Archives[archiveIndex]

	var currentPoints archive

PointLoop:
	for _, point := range points {
		age := now - point.Timestamp

		for currentArchive.Retention() < age {
			if len(currentPoints) > 0 {
				sort.Sort(currentPoints)
				if err := w.archiveUpdateMany(*currentArchive, currentPoints); err != nil {
					return err
				}
				currentPoints = currentPoints[:0]
			}

			archiveIndex += 1
			if archiveIndex < len(w.Header.Archives) {
				currentArchive = &w.Header.Archives[archiveIndex]
			} else {
				// Drop remaining points that don't fit in the db
				currentArchive = nil
				break PointLoop
			}

		}

		currentPoints = append(currentPoints, point)
	}

	if currentArchive != nil && len(currentPoints) > 0 {
		sort.Sort(currentPoints)
		if err := w.archiveUpdateMany(*currentArchive, currentPoints); err != nil {
			return err
		}
	}

	return nil
}

// Fetch is equivalent to calling FetchUntil with until set to time.Now()
func (w *Whisper) Fetch(from uint32) (Interval, []Point, error) {
	now := uint32(time.Now().Unix())
	return w.FetchUntil(from, now)
}

// FetchTime is like Fetch but accepts a time.Time
func (w *Whisper) FetchTime(from time.Time) (Interval, []Point, error) {
	now := uint32(time.Now().Unix())
	return w.FetchUntil(uint32(from.Unix()), now)
}

// FetchUntilTime is like FetchUntil but accepts time.Time
func (w *Whisper) FetchUntilTime(from, until time.Time) (Interval, []Point, error) {
	return w.FetchUntil(uint32(from.Unix()), uint32(until.Unix()))
}

// FetchUntil returns all points between two timestamps
func (w *Whisper) FetchUntil(from, until uint32) (interval Interval, points []Point, err error) {
	now := uint32(time.Now().Unix())

	// Tidy up the time ranges
	oldest := now - w.Header.Metadata.MaxRetention
	if from < oldest {
		from = oldest
	}
	if from > until {
		err = errors.New("from time is not less than until time")
		return
	}
	if until > now {
		until = now
	}

	// Find the archive with enough retention to get be holding our data
	var archive ArchiveInfo
	diff := now - from
	for _, info := range w.Header.Archives {
		if info.Retention() >= diff {
			archive = info
			break
		}
	}

	step := archive.SecondsPerPoint
	fromTimestamp := quantize(from, step) + step
	fromOffset, err := w.pointOffset(archive, fromTimestamp)
	if err != nil {
		return
	}

	untilTimestamp := quantize(until, step) + step
	untilOffset, err := w.pointOffset(archive, untilTimestamp)
	if err != nil {
		return
	}

	rawPoints, err := w.readPointsBetweenOffsets(archive, fromOffset, untilOffset)
	if err != nil {
		return
	}

	currentInterval := fromTimestamp
	for _, p := range rawPoints {
		if p.Timestamp == currentInterval {
			points = append(points, p)
		}
		currentInterval += step
	}

	interval = Interval{fromTimestamp, untilTimestamp, step}
	return
}

func (w *Whisper) archiveUpdateMany(archiveInfo ArchiveInfo, points archive) (err error) {
	type stampedArchive struct {
		timestamp uint32
		points    archive
	}
	var archives []stampedArchive
	var currentPoints archive
	var previousTimestamp, archiveStart uint32

	step := archiveInfo.SecondsPerPoint
	points = quantizeArchive(points, step)

	for _, point := range points {
		if point.Timestamp == previousTimestamp {
			// ignore values with duplicate timestamps
			continue
		}

		if (previousTimestamp != 0) && (point.Timestamp != previousTimestamp+step) {
			// the current point is not contiguous to the last, start a new series of points

			// append the current archive to the archive list
			archiveStart = previousTimestamp - (uint32(len(currentPoints)) * step)
			archives = append(archives, stampedArchive{archiveStart, currentPoints})

			// start a new archive
			currentPoints = archive{}
		}

		currentPoints = append(currentPoints, point)
		previousTimestamp = point.Timestamp

	}

	if len(currentPoints) > 0 {
		// If there are any more points remaining after the loop, make a new series for them as well
		archiveStart = previousTimestamp - (uint32(len(currentPoints)) * step)
		archives = append(archives, stampedArchive{archiveStart, currentPoints})
	}

	for _, archive := range archives {
		err = w.writeArchive(archiveInfo, archive.points...)
		if err != nil {
			return err
		}
	}

	higher := archiveInfo

PropagateLoop:
	for _, info := range w.Header.Archives {
		if info.SecondsPerPoint <= archiveInfo.SecondsPerPoint {
			continue
		}

		quantizedPoints := quantizeArchive(points, info.SecondsPerPoint)
		lastPoint := Point{0, 0}
		for _, point := range quantizedPoints {
			if point.Timestamp == lastPoint.Timestamp {
				continue
			}

			propagateFurther, err := w.propagate(point.Timestamp, higher, info)
			if err != nil {
				return err
			}
			if !propagateFurther {
				break PropagateLoop
			}

			lastPoint = point
		}
		higher = info
	}
	return
}

func (w *Whisper) propagate(timestamp uint32, higher ArchiveInfo, lower ArchiveInfo) (result bool, err error) {
	lowerIntervalStart := quantize(timestamp, lower.SecondsPerPoint)

	// The offset of the first point in the higher resolution data to be propagated down
	higherFirstOffset, err := w.pointOffset(higher, lowerIntervalStart)
	if err != nil {
		return
	}

	// how many higher resolution points that go in to a lower resolution point
	numHigherPoints := lower.SecondsPerPoint / higher.SecondsPerPoint

	// The total size of the higher resolution points
	higherPointsSize := numHigherPoints * pointSize

	// The realtive offset of the first high res point
	relativeFirstOffset := higherFirstOffset - higher.Offset
	// The relative offset of the last high res point
	relativeLastOffset := (relativeFirstOffset + higherPointsSize) % higher.Size()

	// The actual offset of the last high res point
	higherLastOffset := relativeLastOffset + higher.Offset

	points, err := w.readPointsBetweenOffsets(higher, higherFirstOffset, higherLastOffset)
	if err != nil {
		return
	}

	var neighborPoints []Point
	currentInterval := lowerIntervalStart
	for i := 0; i < len(points); i++ {
		if points[i].Timestamp == currentInterval {
			neighborPoints = append(neighborPoints, points[i])
		}
		currentInterval += higher.SecondsPerPoint
	}

	knownPercent := float32(len(neighborPoints))/float32(len(points)) < w.Header.Metadata.XFilesFactor
	if len(neighborPoints) == 0 || knownPercent {
		// There's nothing to propagate
		return false, nil
	}

	aggregatePoint, err := aggregate(w.Header.Metadata.AggregationMethod, neighborPoints)
	if err != nil {
		return
	}
	aggregatePoint.Timestamp = lowerIntervalStart

	err = w.writeArchive(lower, aggregatePoint)

	return true, nil

}

// SetAggregationMethod updates the aggregation method for the database
func (w *Whisper) SetAggregationMethod(m AggregationMethod) error {
	//TODO: Validate the value of aggregationMethod
	meta := w.Header.Metadata
	meta.AggregationMethod = m
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	if err := binary.Write(w.file, binary.BigEndian, meta); err != nil {
		return err
	}
	w.Header.Metadata = meta
	return nil
}

// readPoint reads a single point from an offset in the database.
func (w *Whisper) readPoint(offset uint32) (point Point, err error) {
	points := make([]Point, 1)
	err = w.readPoints(offset, points)
	point = points[0]
	return
}

// readPoints fills a slice of points with data from an offset in the database.
func (w *Whisper) readPoints(offset uint32, points []Point) error {
	_, err := w.file.Seek(int64(offset), 0)
	if err != nil {
		return err
	}
	return binary.Read(w.file, binary.BigEndian, points)
}

// writePoints writes a slice of points at an offset in the database.
func (w *Whisper) writePoints(offset uint32, points []Point) error {
	if _, err := w.file.Seek(int64(offset), 0); err != nil {
		return err
	}
	return binary.Write(w.file, binary.BigEndian, points)
}

func (w *Whisper) readPointsBetweenOffsets(archive ArchiveInfo, startOffset, endOffset uint32) (points []Point, err error) {
	archiveStart := archive.Offset
	archiveEnd := archive.end()
	if startOffset < endOffset {
		// The selection is in the middle of the archive. eg: --####---
		points = make([]Point, (endOffset-startOffset)/pointSize)
		err = w.readPoints(startOffset, points)
	} else {
		// The selection wraps over the end of the archive. eg: ##----###
		numEndPoints := (archiveEnd - startOffset) / pointSize
		numBeginPoints := (endOffset - archiveStart) / pointSize
		points = make([]Point, numBeginPoints+numEndPoints)

		err = w.readPoints(startOffset, points[:numEndPoints])
		if err != nil {
			return
		}
		err = w.readPoints(archiveStart, points[numEndPoints:])
	}
	return
}

// writeArchive write points to an archive.
// It assumes the points are in chronological order and at intervals matching the archive's resolution.
// The offset is determined by the timestamp of the first point.
func (w *Whisper) writeArchive(archive ArchiveInfo, points ...Point) error {
	nPoints := uint32(len(points))

	// Sanity check
	if nPoints > archive.Points {
		return fmt.Errorf("archive can store at most %d points, %d supplied", archive.Points, nPoints)
	}

	offset, err := w.pointOffset(archive, points[0].Timestamp)
	if err != nil {
		return err
	}

	maxPointsFromOffset := (archive.end() - offset) / pointSize
	if nPoints > maxPointsFromOffset {
		// Points span the beginning and end of the archive, eg: ##----###
		if err = w.writePoints(offset, points[:maxPointsFromOffset]); err != nil {
			return err
		}
		err = w.writePoints(archive.Offset, points[maxPointsFromOffset:])
	} else {
		// Points are in the middle of the archive, eg: --####---
		err = w.writePoints(offset, points)
	}

	return err
}

// pointOffset returns the offset of a timestamp within an archive
func (w *Whisper) pointOffset(archive ArchiveInfo, timestamp uint32) (offset uint32, err error) {
	basePoint, err := w.readPoint(archive.Offset)
	if err != nil {
		return 0, err
	}

	if basePoint.Timestamp == 0 {
		// The archive has never been written, this will be the new base point
		return archive.Offset, nil
	}

	totalArchivePeriod := archive.Points * archive.SecondsPerPoint
	var timeDistance uint32
	if timestamp >= basePoint.Timestamp {
		timeDistance = (timestamp - basePoint.Timestamp) % totalArchivePeriod
	} else {
		timeDistance = totalArchivePeriod - ((basePoint.Timestamp - timestamp) % totalArchivePeriod)
	}
	pointDistance := timeDistance / archive.SecondsPerPoint
	byteDistance := pointDistance * pointSize
	return archive.Offset + byteDistance, nil
}

/*
ParseArchiveInfo returns an ArchiveInfo represented by the string.

The string must consist of two numbers, the precision and retention, separated by a colon (:).

Both the precision and retention strings accept a unit suffix. Acceptable suffixes are: "s" for second,
"m" for minute, "h" for hour, "d" for day, "w" for week, and "y" for year.

The precision string specifies how large of a time interval is represented by a single point in the archive.

The retention string specifies how long points are kept in the archive. If no suffix is given for the retention
it is taken to mean a number of points and not a duration.

*/
func ParseArchiveInfo(archiveString string) (ArchiveInfo, error) {
	a := ArchiveInfo{}
	c := strings.Split(archiveString, ":")
	if len(c) != 2 {
		return a, fmt.Errorf("could not parse: %s", archiveString)
	}

	precision := c[0]
	retention := c[1]

	parsedPrecision := precisionRegexp.FindStringSubmatch(precision)
	if parsedPrecision == nil {
		return a, fmt.Errorf("invalid precision string: %s", precision)
	}

	secondsPerPoint, err := parseUint32(parsedPrecision[1])
	if err != nil {
		return a, err
	}

	if parsedPrecision[2] != "" {
		secondsPerPoint, err = expandUnits(secondsPerPoint, parsedPrecision[2])
		if err != nil {
			return a, err
		}
	}

	parsedPoints := precisionRegexp.FindStringSubmatch(retention)
	if parsedPoints == nil {
		return a, fmt.Errorf("invalid retention string: %s", precision)
	}

	points, err := parseUint32(parsedPoints[1])
	if err != nil {
		return a, err
	}

	var retentionSeconds uint32
	if parsedPoints[2] != "" {
		retentionSeconds, err = expandUnits(points, parsedPoints[2])
		if err != nil {
			return a, err
		}
		points = retentionSeconds / secondsPerPoint
	}

	a = ArchiveInfo{0, secondsPerPoint, points}
	return a, nil
}

// quantizeArchive returns a copy of arc with all the points quantized to the given resolution.
func quantizeArchive(arc archive, resolution uint32) archive {
	result := archive{}
	for _, point := range arc {
		result = append(result, Point{quantize(point.Timestamp, resolution), point.Value})
	}
	return result
}

// quantize returns the timestamp quantized to the resloution.
func quantize(timestamp, resolution uint32) uint32 {
	return timestamp - (timestamp % resolution)
}

func aggregate(aggregationMethod AggregationMethod, points []Point) (point Point, err error) {
	switch aggregationMethod {
	case AggregationAverage:
		for _, p := range points {
			point.Value += p.Value
		}
		point.Value /= float64(len(points))
	case AggregationSum:
		for _, p := range points {
			point.Value += p.Value
		}
	case AggregationLast:
		point.Value = points[len(points)-1].Value
	case AggregationMax:
		point.Value = points[0].Value
		for _, p := range points {
			if p.Value > point.Value {
				point.Value = p.Value
			}
		}
	case AggregationMin:
		point.Value = points[0].Value
		for _, p := range points {
			if p.Value < point.Value {
				point.Value = p.Value
			}
		}
	default:
		err = errors.New("unknown aggregation function")
	}
	return
}
