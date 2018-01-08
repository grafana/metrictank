package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type posTracker struct {
	sync.Mutex
	file         string
	fd           *os.File
	completedMap sync.Map
	wg           sync.WaitGroup
}

func NewPositionKeeper(file string) (*posTracker, error) {
	p := &posTracker{file: file}

	fd, err := os.Open(file)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		reader := bufio.NewReader(fd)
		var path string
		for {
			line, isPrefix, err := reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			path += string(line)
			if isPrefix {
				continue
			} else {
				p.completedMap.Store(path, struct{}{})
				path = ""
			}
		}

		fd.Close()
	}

	p.fd, err = os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *posTracker) IsDone(path string) bool {
	_, ok := p.completedMap.Load(path)
	return ok
}

func (p *posTracker) Done(path string) {
	p.completedMap.Store(path, struct{}{})
	p.wg.Add(1)
	go func() {
		p.Lock()
		defer p.Unlock()
		p.fd.WriteString(fmt.Sprintf("%s\n", path))
		p.fd.Sync()
		p.wg.Done()
	}()
}

func (p *posTracker) Close() {
	p.wg.Wait()
	p.fd.Close()
}
