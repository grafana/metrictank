package main

import (
	"os"
	"testing"
)

func TestPositionKeeper(t *testing.T) {
	filePath := "/tmp/positionKeeperTest"
	clearFile := func() { os.Remove(filePath) }
	clearFile()
	defer clearFile()

	testValue1 := "file1"
	testValue2 := "file2"
	testValue3 := "file3"

	p1, err := NewPositionKeeper(filePath)
	if err != nil {
		t.Fatalf("Error instantiating position keeper: %s", err)
	}
	p1.Done(testValue1)
	if !p1.IsDone(testValue1) {
		t.Fatalf("Expected %s to be done, but it was not", testValue1)
	}
	if p1.IsDone(testValue2) {
		t.Fatalf("Expected %s to not be done, but it was", testValue2)
	}
	p1.Done(testValue2)
	if !p1.IsDone(testValue2) {
		t.Fatalf("Expected %s to be done, but it was not", testValue2)
	}
	p1.Close()

	// read the file into new instance of position keeper
	p2, err := NewPositionKeeper(filePath)
	if err != nil {
		t.Fatalf("Error instantiating position keeper: %s", err)
	}
	if !p2.IsDone(testValue1) || !p2.IsDone(testValue2) {
		t.Fatalf("Expected %s and %s to be done, but it was not", testValue1, testValue2)
	}

	if p2.IsDone(testValue3) {
		t.Fatalf("Expected %s to not be done, but it was", testValue3)
	}

	p2.Done(testValue3)
	p2.Close()

	// read the file into new instance of position keeper
	p3, err := NewPositionKeeper(filePath)
	if err != nil {
		t.Fatalf("Error instantiating position keeper: %s", err)
	}
	if !p3.IsDone(testValue1) || !p3.IsDone(testValue2) || !p3.IsDone(testValue3) {
		t.Fatalf("Expected %s, %s and %s to be done, but it was not", testValue1, testValue2, testValue3)
	}
}
