package transport

import (
	"encoding/binary"
	"errors"
	"io"
)

type Frame struct {
	Type    byte
	Payload []byte
}

func WriteFrame(w io.Writer, msgType byte, payload []byte) error {
	buf := make([]byte, 8+len(payload))

	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = msgType
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))

	copy(buf[8:], payload)

	_, err := w.Write(buf)
	return err
}

func ReadFrame(r io.Reader) (Frame, error) {
	header := make([]byte, 8)

	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, err
	}

	magic := binary.BigEndian.Uint16(header[0:2])
	if magic != Magic {
		return Frame{}, errors.New("invalid magic")
	}

	length := binary.BigEndian.Uint32(header[4:8])
	msgType := header[3]

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}

	return Frame{
		Type:    msgType,
		Payload: payload,
	}, nil
}