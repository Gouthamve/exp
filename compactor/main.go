package main

import (
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"math/rand"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/go-kit/kit/log"
	promlabels "github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

func main() {
	makeBlocks(5, 300000, 2*60*60*1000, 5000)

	c := tsdb.NewCompactor("./blocks", nil, log.NewLogfmtLogger(os.Stdout), nil)

	dirs, err := getDirs("./blocks")
	if err != nil {
		golog.Fatalln(err)
	}
	fmt.Println(c.Compact(dirs...))
}

type seriesMap map[uint64][]labels.Labels
type noopWAL struct{}

func (nw noopWAL) Reader() tsdb.WALReader                                   { return nw }
func (nw noopWAL) LogSeries([]labels.Labels) error                          { return nil }
func (nw noopWAL) LogSamples([]tsdb.RefSample) error                        { return nil }
func (nw noopWAL) LogDeletes([]tsdb.Stone) error                            { return nil }
func (nw noopWAL) Close() error                                             { return nil }
func (nw noopWAL) Read(tsdb.SeriesCB, tsdb.SamplesCB, tsdb.DeletesCB) error { return nil }

func makeBlocks(numBlocks, numSeries, blockSize, scrapeInterval int64) ([]string, error) {
	t0 := int64(0)
	numSamples := blockSize / scrapeInterval

	lFile, err := os.Open("labels")
	if err != nil {
		panic(err)
	}

	sMap, err := getSeries(lFile, int(numSeries))
	if err != nil {
		panic(err)
	}
	lFile.Close()

	c := tsdb.NewCompactor("./blocks", nil, log.NewLogfmtLogger(os.Stdout), nil)

	for i := int64(0); i < numBlocks; i++ {
		// Create a block.
		dir, err := tsdb.TouchHeadBlock("./blocks", t0, t0+blockSize)
		if err != nil {
			panic(err)
		}
		refs := make([]string, 0, numSeries)

		h, err := tsdb.OpenHeadBlock(dir, log.NewLogfmtLogger(os.Stdout), noopWAL{})
		// Add samples.
		t := t0
		a := h.Appender()

		// Commit everything once to get the refs.
		for _, s := range sMap {
			for _, metric := range s {
				_, err := a.Add(metric, t, rand.Float64())
				if err != nil {
					panic(err)
				}
			}
		}
		if err := a.Commit(); err != nil {
			panic(err)
		}
		t += scrapeInterval
		a = h.Appender()
		// Store the refs now.
		for _, s := range sMap {
			for _, metric := range s {
				ref, err := a.Add(metric, t, rand.Float64())
				if err != nil {
					panic(err)
				}
				if ref == "" {
					panic("empty ref")
				}

				refs = append(refs, ref)
			}
		}
		if err := a.Commit(); err != nil {
			panic(err)
		}
		t += scrapeInterval

		refChan := make(chan string)

		for i := 0; i < 10; i++ {
			go func(t2 int64) {
				for ref := range refChan {
					t := t2
					a := h.Appender()
					for j := int64(2); j < numSamples; j++ {
						if err := a.AddFast(ref, t, float64(j)); err != nil {
							panic(err)
						}

						t += scrapeInterval
					}

					if err := a.Commit(); err != nil {
						panic(err)
					}
				}
			}(t)
		}

		i := 0
		for _, ref := range refs {
			refChan <- ref
			i++

			if i%10000 == 0 {
				fmt.Println("Series Done:", i)
			}
		}

		// Compact and close block.
		if err := c.Write(h); err != nil {
			panic(err)
		}
		if err := h.Close(); err != nil {
			panic(err)
		}

		t0 += blockSize
	}

	return nil, nil
}

func getSeries(r io.Reader, n int) (seriesMap, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	p := textparse.New(b)
	i := 0
	sMap := make(seriesMap)

	for p.Next() && i < n {
		m := make(labels.Labels, 0, 10)
		p.Metric((*promlabels.Labels)(unsafe.Pointer(&m)))
		h := m.Hash()
		if get(sMap, h, m) {
			continue
		}

		sMap[h] = append(sMap[h], m)
		i++
	}

	return sMap, nil
}

func get(m seriesMap, hash uint64, ls labels.Labels) bool {
	series := m[hash]
	for _, s := range series {
		if s.Equals(ls) {
			return true
		}
	}

	return false

}

func getDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	dirs := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, filepath.Join(dir, file.Name()))
		}
	}

	return dirs, nil
}
