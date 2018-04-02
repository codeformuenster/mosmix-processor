package db

import (
	"encoding/xml"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (k *KMLPoint) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var kmlPoint string

	if err := d.DecodeElement(&kmlPoint, &start); err != nil {
		return err
	}

	parts := strings.Split(kmlPoint, ",")
	if len(parts) != 3 {
		return errors.New("too few coordinate parts")
	}

	var lon, lat, altitude float64

	lon, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[0], parts)
	}
	lat, err = strconv.ParseFloat(parts[1], 64)
	if err != nil {
		fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[1], parts)
	}

	altitude, err = strconv.ParseFloat(parts[2], 64)
	if err != nil {
		fmt.Printf("Coordinate %v of coordinates %v cannot be parsed as float", parts[2], parts)
	}

	*k = KMLPoint{lon, lat, altitude}

	return nil
}
