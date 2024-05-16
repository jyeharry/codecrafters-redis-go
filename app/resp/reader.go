package resp

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"

	pkgerrors "github.com/pkg/errors"
)

const (
	SIMPLE_STRING = '+'
	ERROR         = '-'
	INTEGER       = ':'
	BULK_STRING   = '$'
	ARRAY         = '*'
)

var separator = []byte("\r\n")

type Reader struct {
	scanner *bufio.Scanner
}

func NewReader(r io.Reader) *Reader {
	scanner := bufio.NewScanner(bufio.NewReaderSize(r, 9192))
	scanner.Split(redisSplitter)

	return &Reader{
		scanner: scanner,
	}
}

func (r *Reader) Read() (*Result, error) {
	return readRESP(r.scanner)
}

func redisSplitter(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) < 3 {
		if atEOF {
			return 0, nil, fmt.Errorf("unexpected end of stream, a redis message needs at least 3 characters to be valid, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data))
		}

		return 0, nil, nil
	}

	found := bytes.Index(data, separator)
	if found == -1 {
		if atEOF {
			return 0, nil, fmt.Errorf("unexpected end of stream, there should have been a \\r\\n before the end, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data))
		}
		return 0, nil, nil
	}

	if data[0] == BULK_STRING {
		length, err := strconv.ParseInt(string(data[1:found]), 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("message starts as bulk string but length is not a valid int, actual content in base64: [%v]", base64.RawStdEncoding.EncodeToString(data[0:found]))
		}

		if length == -1 {
			return 5, []byte("$"), nil
		}

		if length == 0 {
			return 6, []byte("+"), nil
		}

		expectedEnding := found + len(separator) + int(length) + len(separator)
		if len(data) >= expectedEnding {
			start := found + 1
			data[start] = '+'
			return expectedEnding, data[start : expectedEnding-2], nil
		}

		if atEOF {
			return 0, nil, fmt.Errorf("unexpected end of stream, stream ends before bulk string has ended, expected there to be %v total bytes but there were only %v, actual content in base64: %v", expectedEnding, len(data), base64.RawStdEncoding.EncodeToString(data))
		}

		return 0, nil, err
	}

	return found + len(separator), data[:found], nil
}

func readRESP(r *bufio.Scanner) (*Result, error) {
	for r.Scan() {
		line := r.Text()
		switch line[0] {
		case SIMPLE_STRING:
			return &Result{
				content: line[1:],
			}, nil
		case BULK_STRING:
			return &Result{
				content: nil,
			}, nil
		case ERROR:
			return &Result{
				content: errors.New(line[1:]),
			}, nil
		case INTEGER:
			content, err := strconv.ParseInt(line[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse returned integer: %v (value: %v)", err, line)
			}
			return &Result{
				content: content,
			}, nil
		case ARRAY:
			length, err := strconv.ParseInt(line[1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse array length: %v (value: %v)", err, line)
			}

			if length == -1 {
				return &Result{content: nil}, nil
			}

			contents := make([]interface{}, 0, length)

			for x := int64(0); x < length; x++ {
				result, err := readRESP(r)
				if err != nil {
					return nil, pkgerrors.Wrapf(err, "failed to read item %v from array", x)
				}

				contents = append(contents, result.content)
			}

			return &Result{
				content: contents,
			}, nil
		}
	}

	if r.Err() == nil {
		return nil, errors.New("scanner was empty")
	}

	return nil, r.Err()
}
