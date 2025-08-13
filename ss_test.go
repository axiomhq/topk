package topk

import (
	// "fmt"
	"math"
	"sort"
	"testing"
)

// TestStreamSummaryBasicInsertion tests basic insertion and frequency tracking
func TestStreamSummaryBasicInsertion(t *testing.T) {
	// Initialize StreamSummary with capacity of 5
	ss := NewStreamSummary(5)

	// Test 1: Insert elements and verify basic structure
	ss.Insert("apple")
	// ss.Print()
	ss.Insert("banana")
	// ss.Print()
	ss.Insert("apple") // apple should now have freq 2
	// ss.Print()
	ss.Insert("orange")
	// ss.Print()
	ss.Insert("banana") // banana should now have freq 2
	// ss.Print()
	ss.Insert("apple") // apple should now have freq 3
	// ss.Print()

	// Verify size
	if ss.size != 3 {
		t.Errorf("Expected size 3, got %d", ss.size)
	}

	// Get TopK results
	topK := ss.TopK(3)

	// Verify we got results
	if topK == nil {
		t.Fatal("TopK returned nil")
	}

	if len(topK.results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(topK.results))
	}

	// Create a map for easier verification
	resultMap := make(map[string]Result)
	for _, result := range topK.results {
		resultMap[result.phrase] = result
	}

	// Verify apple frequency (should be 3)
	if apple, exists := resultMap["apple"]; exists {
		if apple.freq != 3 {
			t.Errorf("Expected apple frequency 3, got %d", apple.freq)
		}
	} else {
		t.Error("Apple not found in results")
	}

	// Verify banana frequency (should be 2)
	if banana, exists := resultMap["banana"]; exists {
		if banana.freq != 2 {
			t.Errorf("Expected banana frequency 2, got %d", banana.freq)
		}
	} else {
		t.Error("Banana not found in results")
	}

	// Verify orange frequency (should be 1)
	if orange, exists := resultMap["orange"]; exists {
		if orange.freq != 1 {
			t.Errorf("Expected orange frequency 1, got %d", orange.freq)
		}
	} else {
		t.Error("Orange not found in results")
	}

	// Test ordering - results should be ordered by frequency (highest first)
	if len(topK.results) >= 2 {
		if topK.results[0].freq < topK.results[1].freq {
			t.Error("Results are not properly ordered by frequency")
		}
	}
}

// TestStreamSummaryCapacityAndEviction tests capacity limits and eviction behavior
func TestStreamSummaryCapacityAndEviction(t *testing.T) {
	// Initialize StreamSummary with small capacity of 3
	ss := NewStreamSummary(3)

	// Insert more elements than capacity to test eviction
	ss.Insert("apple")  // freq 1
	ss.Insert("banana") // freq 1
	ss.Insert("orange") // freq 1

	// At this point we're at capacity (3/3)
	if ss.size != 3 {
		t.Errorf("Expected size 3 after initial inserts, got %d", ss.size)
	}

	// Now insert a new element - this should trigger eviction
	ss.Insert("grape") // This should evict one of the existing elements

	// Size should still be 3 (capacity limit)
	if ss.size != 3 {
		t.Errorf("Expected size to remain 3 after eviction, got %d", ss.size)
	}

	// Insert more occurrences to create frequency differences
	ss.Insert("apple")  // apple freq should be 2
	ss.Insert("apple")  // apple freq should be 3
	ss.Insert("banana") // banana freq should be 2
	ss.Insert("grape")  // grape freq should be 2

	// Get TopK results
	topK := ss.TopK(3)

	if topK == nil {
		t.Fatal("TopK returned nil")
	}

	// Should have exactly 3 results (our capacity)
	if len(topK.results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(topK.results))
	}

	// Create result map for verification
	resultMap := make(map[string]Result)
	for _, result := range topK.results {
		resultMap[result.phrase] = result
	}

	// Apple should have highest frequency
	if apple, exists := resultMap["apple"]; exists {
		if apple.freq < 2 { // Should be at least 2, might be higher due to eviction algorithm
			t.Errorf("Expected apple frequency >= 2, got %d", apple.freq)
		}
	} else {
		t.Error("Apple should still be tracked (high frequency)")
	}

	// Verify that we don't have more than 3 distinct elements
	distinctElements := make(map[string]bool)
	for _, result := range topK.results {
		distinctElements[result.phrase] = true
	}

	if len(distinctElements) > 3 {
		t.Errorf("Expected at most 3 distinct elements, got %d", len(distinctElements))
	}

	// Test error bounds (alpha values)
	for _, result := range topK.results {
		// Alpha should be non-negative
		if result.alpha < 0 {
			t.Errorf("Alpha value should be non-negative, got %d for %s", result.alpha, result.phrase)
		}

		// Frequency should be greater than or equal to alpha
		if result.freq < result.alpha {
			t.Errorf("Frequency (%d) should be >= alpha (%d) for %s", result.freq, result.alpha, result.phrase)
		}
	}

	// Test TopK with k < capacity
	topK2 := ss.TopK(2)
	if len(topK2.results) != 2 {
		t.Errorf("Expected 2 results when k=2, got %d", len(topK2.results))
	}

	// Verify ordering in limited TopK
	if len(topK2.results) == 2 {
		if topK2.results[0].freq < topK2.results[1].freq {
			t.Error("TopK(2) results are not properly ordered by frequency")
		}
	}
}

// Helper function to verify bucket structure integrity (optional debug test)
func TestStreamSummaryBucketIntegrity(t *testing.T) {
	ss := NewStreamSummary(4)

	// Insert elements to create multiple buckets
	ss.Insert("a")
	ss.Insert("b")
	ss.Insert("a") // Should move 'a' to frequency 2 bucket
	ss.Insert("c")
	ss.Insert("a") // Should move 'a' to frequency 3 bucket

	// Verify that bucket1 points to the minimum frequency bucket
	if ss.bucketMin == nil {
		t.Fatal("bucket1 should not be nil")
	}

	// Verify bucket chain is properly linked
	bucket := ss.bucketMin
	prevValue := uint64(0)
	bucketCount := 0

	for bucket != nil && bucketCount < 10 { // Safety limit to prevent infinite loops
		if bucket.value <= prevValue && prevValue != 0 {
			t.Errorf("Bucket values should be increasing, got %d after %d", bucket.value, prevValue)
		}
		prevValue = bucket.value
		bucket = bucket.next
		bucketCount++
	}

	// Verify all tracked elements are in buckets map
	if len(ss.buckets) != ss.size {
		t.Errorf("Buckets map size (%d) should match ss.size (%d)", len(ss.buckets), ss.size)
	}

	// Verify all tracked elements are in ids map
	if len(ss.ids) != ss.size {
		t.Errorf("IDs map size (%d) should match ss.size (%d)", len(ss.ids), ss.size)
	}
}

func TestText(t *testing.T) {
	text := []string{
		"hello world", "thank you", "good morning", "how are you", "see you later", "please help", "yes please", "no thanks",
		"excuse me", "sorry about that", "null pointer exception", "segmentation fault", "hello world", "foo bar", "test test",
		"lorem ipsum", "todo fix this", "it works", "bug report", "feature request", "access denied", "file not found", "connection timeout",
		"invalid input", "permission denied", "out of memory", "syntax error", "page not found", "server error", "bad request", "click here",
		"sign up", "log in", "log out", "add to cart", "check out", "submit form", "cancel order", "save changes", "delete account", "in progress",
		"completed successfully", "pending approval", "under review", "processing request", "loading data", "please wait", "almost done", "ready to go",
		"all done", "test", "test test", "test test test", "data", "data data", "sample", "example", "demo", "trial", "beta", "ok", "yes", "no", "maybe", 
		"confirmed", "acknowledged", "understanding confirmed", "message received and understood", "i", "we", "best regards", "kind regards", "thank you for your email", 
		"following up", "as discussed", "please find attached", "for your review", "action required", "urgent matter", "meeting scheduled", "um okay", "you know", "i mean",
		"basically", "actually", "literally", "to be honest", "in my opinion", "just saying", "whatever", "important", "important", "important", "critical", "critical", 
		"high priority", "high priority", "high priority", "high priority",
	}
	actualTop10 := make(map[string]uint64)
	for _, t := range text {
		actualTop10[t]++
	}
	actualTop10List := make([]struct{
		phrase string
		freq uint64
	}, 0, len(actualTop10))
	for k := range actualTop10 {
		actualTop10List = append(actualTop10List, struct{
			phrase string
			freq uint64
		}{
			phrase: k,
			freq: actualTop10[k],
		})
	}
	sort.Slice(actualTop10List, func(i, j int) bool {
		return actualTop10List[i].freq > actualTop10List[j].freq
	})
	cnt := 0
	for i, t := range actualTop10List {
		if cnt >= 10 && t.freq != actualTop10List[i - 1].freq {
			break
		}
		cnt++
	}

	actualTop10List = actualTop10List[:cnt]
	actualTop10 = make(map[string]uint64)
	for _, t := range actualTop10List {
		actualTop10[t.phrase] = t.freq
	}
	ss := NewStreamSummary(10)
	for _, t := range text {
		// fmt.Println("Inserting", i, t)
		ss.Insert(t)
	}
	topK := ss.TopK(10)
	// fmt.Println(len(topK.results))
	avgError := uint64(0)
	for _, r := range topK.results {
		if actualTop10[r.phrase] != 0 {
			// fmt.Println(r.phrase, actualTop10[r.phrase], r.freq - r.alpha)
			avgError += uint64(math.Abs(float64(actualTop10[r.phrase] - (r.freq - r.alpha))))
		}
	}
	// fmt.Println("Average error:", float64(avgError) / float64(len(topK.results)))
	// fmt.Println("Top 10:", topK.resalTop1ults)
	// fmt.Println("Actual top 10:", actu0List)
}