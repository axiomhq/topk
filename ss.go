/*
Stream Summary is implemented following the paper:
https://home.cse.ust.hk/~raywong/comp5331/References/EfficientComputationOfFrequentAndTop-kElementsInDataStreams.pdf
*/

package topk

import (
	"fmt"

	"github.com/dgryski/go-metro"
)

const MAX_UINT64 = ^uint64(0)

type Result struct {
	phrase string
	freq   uint64
	alpha  uint64
}

type TopKResult struct {
	ordered           bool
	guaranteed        bool
	minGuaranteedFreq uint64
	results           []Result
}

type BucketElement struct {
	id    uint64
	alpha uint64
	next  *BucketElement
}

func NewBucketElement(id uint64, alpha uint64) *BucketElement {
	e := &BucketElement{
		id:    id,
		alpha: alpha,
		next:  nil,
	}
	e.next = e
	return e
}

func (e *BucketElement) SetNext(next *BucketElement) {
	e.next = next
}

func (e *BucketElement) GetID() uint64 {
	return e.id
}

func (e *BucketElement) GetAlpha() uint64 {
	return e.alpha
}

type Bucket struct {
	value    uint64
	next     *Bucket
	prev     *Bucket
	elements *BucketElement // pointer to the first element in the bucket
}

func NewBucket(value uint64) *Bucket {
	return &Bucket{
		value:    value,
		next:     nil,
		prev:     nil,
		elements: nil,
	}
}

func (b *Bucket) SetNext(next *Bucket) {
	b.next = next
}

func (b *Bucket) SetPrev(prev *Bucket) {
	b.prev = prev
}

func (b *Bucket) Insert(e *BucketElement) {
	if b.elements == nil {
		b.elements = e
		e.next = e // link the element to itself
		return
	}

	e.next = b.elements // e will be the new start of the list
	curr := b.elements
	for curr.next != b.elements { // reach the end of the list
		curr = curr.next
	}
	curr.next = e  // link the last element to e (circular)
	b.elements = e // update the start of the list
}

func (b *Bucket) Remove(e *BucketElement) {
	curr := b.elements
	if curr == e {
		for curr.next != b.elements {
			curr = curr.next
		}
		// if e is the only element
		if e == e.next {
			b.elements = nil
			e.next = nil // unlink the element from the bucket
			return
		}
		b.elements = e.next
		curr.next = b.elements
		e.next = nil // unlink the element from the bucket
		return
	}

	for curr.next != b.elements {
		if curr.next == e {
			curr.next = e.next
			e.next = nil // unlink the element from the bucket
			return
		}
		curr = curr.next
	}

	if curr.next == e {
		curr.next = b.elements // link the last element to the start of the list
		e.next = nil // unlink the element from the bucket
	}
}

func (b *Bucket) Evict() *BucketElement { // evict the first element wiht max alpha
	curr := b.elements
	maxAlpha := curr.alpha
	maxAlphaElement := curr
	for curr.next != b.elements {
		if curr.alpha > maxAlpha {
			maxAlpha = curr.alpha
			maxAlphaElement = curr
		}
		curr = curr.next
	}
	b.Remove(maxAlphaElement)
	return maxAlphaElement
}

func (b *Bucket) Find(id uint64) *BucketElement {
	curr := b.elements
	for curr.next != b.elements {
		if curr.id == id {
			return curr
		}
		curr = curr.next
	}
	if curr.id == id { // check the last element
		return curr
	}
	return nil
}

func (b *Bucket) Move(id uint64) *Bucket {
	found := b.Find(id)
	if found == nil {
		return nil
	}

	// Remove element from current bucket
	b.Remove(found)

	// Target frequency is current + 1
	targetValue := b.value + 1

	// Check if previous bucket (higher frequency) has the target value
	if b.prev != nil && b.prev.value == targetValue {
		// Move to existing higher frequency bucket
		b.prev.Insert(found)
		return b.prev
	}

	// Need to create new bucket with target frequency
	newBucket := NewBucket(targetValue)
	newBucket.Insert(found)

	// Insert new bucket between current bucket and its previous bucket
	// Chain ordering: ... <- prev <- newBucket <- current <- next -> ...
	// (Higher freq) <- (Lower freq)

	newBucket.next = b
	newBucket.prev = b.prev

	if b.prev != nil {
		b.prev.next = newBucket
	}
	b.prev = newBucket

	return newBucket
}

type StreamSummary struct {
	m         int                // max number of monitored phrases
	size      int                // current number of monitored phrases
	buckets   map[uint64]*Bucket // map from ids to buckets
	ids       map[uint64]string  // map from id to phrases
	bucketMax *Bucket            // pointer to the maximum frequency bucket (head of chain)
	bucketMin *Bucket            // pointer to the minimum frequency bucket (tail of chain)
}

func NewStreamSummary(m int) *StreamSummary {
	// Start with a single bucket for frequency 1
	initialBucket := NewBucket(1)
	return &StreamSummary{
		m:         m,
		size:      0,
		buckets:   make(map[uint64]*Bucket),
		ids:       make(map[uint64]string),
		bucketMax: initialBucket, // Initially, min and max point to the same bucket
		bucketMin: initialBucket,
	}
}

// Helper method to update bucketMax pointer when a new maximum bucket is created
func (ss *StreamSummary) updateBucketMax(newBucket *Bucket) {
	if ss.bucketMax == nil || newBucket.value > ss.bucketMax.value {
		ss.bucketMax = newBucket
	}
}

// Helper method to update bucketMin pointer when minimum bucket changes
func (ss *StreamSummary) updateBucketMin() {
	if ss.bucketMin != nil && ss.bucketMin.elements == nil {
		// Current min bucket is empty, find new minimum
		ss.bucketMin = ss.bucketMin.prev
	}
}

// Helper method to remove empty bucket from chain
func (ss *StreamSummary) removeEmptyBucket(bucket *Bucket) {
	if bucket.elements != nil {
		return // Not empty
	}

	// Update pointers if this is min or max bucket
	if ss.bucketMin == bucket {
		ss.bucketMin = bucket.prev
	}
	if ss.bucketMax == bucket {
		ss.bucketMax = bucket.next
	}

	// Remove from chain
	if bucket.prev != nil {
		bucket.prev.next = bucket.next
	}
	if bucket.next != nil {
		bucket.next.prev = bucket.prev
	}
}

func (ss *StreamSummary) Insert(phrase string) {
	id := ss.Hash(phrase)
	// fmt.Println("Inserting phrase:", phrase, "with id:", id)
	if bucket, ok := ss.buckets[id]; ok { // phrase already monitored
		// Move element to higher frequency bucket
		newBucket := bucket.Move(id)
		if newBucket != nil {
			ss.buckets[id] = newBucket
			ss.updateBucketMax(newBucket)
		}

		// Clean up empty bucket
		ss.removeEmptyBucket(bucket)
		ss.updateBucketMin()
		// fmt.Println("bucket", bucket.value)
		// if newBucket != nil {
		// 	fmt.Println("newBucket", newBucket.value)
		// 	fmt.Println("newBucket elements", newBucket.elements.id)
		// } else {
		// 	fmt.Println("newBucket is nil")
		// }
		ss.ids[id] = phrase
		return
	}

	// if the phrase is not monitored, and there is still space add it
	if ss.size < ss.m {
		// Add to minimum frequency bucket (frequency 1)
		if ss.bucketMin == nil || ss.bucketMin.value != 1 {
			// Create bucket with frequency 1
			newBucket := NewBucket(1)
			if ss.bucketMin != nil {
				newBucket.prev = ss.bucketMin
				ss.bucketMin.next = newBucket
			}
			ss.bucketMin = newBucket
			if ss.bucketMax == nil {
				ss.bucketMax = newBucket
			}
		}

		ss.bucketMin.Insert(NewBucketElement(id, 0))
		ss.buckets[id] = ss.bucketMin
		ss.ids[id] = phrase
		ss.size++
		return
	}

	// if the phrase is not monitored, and there is no space, evict the least frequent phrase
	evicted := ss.bucketMin.Evict()
	delete(ss.buckets, evicted.id)
	delete(ss.ids, evicted.id)

	// Insert new element with error bound
	newElement := NewBucketElement(id, ss.bucketMin.value)
	ss.bucketMin.Insert(newElement)

	// Move to next frequency bucket
	newBucket := ss.bucketMin.Move(id)
	if newBucket != nil {
		ss.buckets[id] = newBucket
		ss.updateBucketMax(newBucket)
	}

	// Clean up empty minimum bucket
	ss.removeEmptyBucket(ss.bucketMin)
	ss.updateBucketMin()

	ss.ids[id] = phrase
}

func (ss *StreamSummary) TopK(k int) *TopKResult {
	ordered := true
	guaranteed := false
	minGuaranteedFreq := MAX_UINT64

	// Start from the maximum frequency bucket (highest frequency first)
	currBucket := ss.bucketMax
	if currBucket == nil {
		return &TopKResult{
			ordered:           ordered,
			guaranteed:        guaranteed,
			minGuaranteedFreq: minGuaranteedFreq,
			results:           []Result{},
		}
	}

	currElement := currBucket.elements
	id := make([]uint64, 0, min(k, ss.size))

	for i := 0; i < min(k, ss.size); i++ {
		if currBucket == nil {
			break
		}
		if currElement == nil {
			// Move to next bucket (lower frequency)
			currBucket = currBucket.next
			if currBucket == nil {
				break
			}
			currElement = currBucket.elements
			if currElement == nil {
				break
			}
		}

		id = append(id, currElement.id)
		if currBucket.value-currElement.alpha < minGuaranteedFreq {
			minGuaranteedFreq = currBucket.value - currElement.alpha
		}

		if currBucket.next != nil &&
			currBucket.value-currElement.alpha < currBucket.next.value {
			ordered = false
		}

		currElement = currElement.next
		if currElement == currBucket.elements {
			// We've gone full circle in the circular list
			currElement = nil
		}
	}

	if currBucket == nil {
		return ss.materialize(id, ordered, guaranteed, minGuaranteedFreq)
	}

	if currElement == nil {
		// Move to next bucket (lower frequency)
		currBucket = currBucket.next
		if currBucket == nil {
			return ss.materialize(id, ordered, guaranteed, minGuaranteedFreq)
		}
		currElement = currBucket.elements
		if currElement == nil {
			return ss.materialize(id, ordered, guaranteed, minGuaranteedFreq)
		}
	}

	if currBucket.value <= minGuaranteedFreq {
		guaranteed = true
	} else {
		id = append(id, currElement.id)
		prevBucket := currBucket
		prevElement := currElement
		currElement = currElement.next
		if currElement == currBucket.elements {
			currElement = nil
		}

		for i := k + 2; i < ss.size; i++ {
			if currElement == nil {
				currBucket = currBucket.next
				if currBucket == nil {
					break
				}
				currElement = currBucket.elements
				if currElement == nil {
					break
				}
			}
			if prevBucket.value - prevElement.alpha < minGuaranteedFreq {
				minGuaranteedFreq = prevBucket.value - prevElement.alpha
			}
			if currBucket.value <= minGuaranteedFreq {
				guaranteed = true
				break
			}
			id = append(id, currElement.id)
			prevBucket = currBucket
			prevElement = currElement
			currElement = currElement.next
			if currElement == currBucket.elements {
				currElement = nil
			}
		}
	}

	return ss.materialize(id, ordered, guaranteed, minGuaranteedFreq)
}

func (ss *StreamSummary) materialize(ids []uint64, ordered bool, guaranteed bool, minGuaranteedFreq uint64) *TopKResult {
	results := make([]Result, 0, len(ids))
	for _, id := range ids {
		phrase := ss.ids[id]
		freq := ss.buckets[id].value
		alpha := ss.buckets[id].Find(id).alpha
		results = append(results, Result{phrase: phrase, freq: freq, alpha: alpha})
	}
	return &TopKResult{
		ordered:           ordered,
		guaranteed:        guaranteed,
		minGuaranteedFreq: minGuaranteedFreq,
		results:           results,
	}
}

func (ss *StreamSummary) Hash(phrase string) uint64 {
	hash := metro.Hash64Str(phrase, uint64(ss.m))
	return hash
}

func (ss *StreamSummary) Print() {
	curr := ss.bucketMax
	for curr != nil {
		fmt.Println("bucket", curr.value)
		currElement := curr.elements
		for currElement.next != curr.elements {
			fmt.Println("element", currElement.id)
			currElement = currElement.next
		}
		fmt.Println("element", currElement.id)
		curr = curr.next
	}
}