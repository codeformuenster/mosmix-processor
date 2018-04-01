package mosmix

// import (
// 	"bufio"
// 	"bytes"
// 	"encoding/xml"
// 	"fmt"
// 	"io/ioutil"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"time"

// 	getter "github.com/hashicorp/go-getter"
// 	"github.com/tidwall/buntdb"
// 	"golang.org/x/net/html/charset"
// )

// type mosmix struct {
// 	db         *buntdb.DB
// 	url        string
// 	xmlDecoder *xml.Decoder
// }

// // NewMosmix instantiates a new mosmix with the given buntdb instance and
// // download url
// func NewMosmix(db *buntdb.DB, url string) *mosmix {
// 	m := mosmix{db: db, url: url}
// 	m.createIndexes()

// 	return &m
// }

// func (m *mosmix) Metadata() (string, error) {
// 	var str string
// 	err := m.db.View(func(tx *buntdb.Tx) error {
// 		dbStr, err := tx.Get("meta:issuer")
// 		if err != nil {
// 			return err
// 		}
// 		str = dbStr
// 		return nil
// 	})
// 	return str, err
// }

// func (m *mosmix) GetNearbyForecasts(lonLat string, numResults int) (string, error) {
// 	var outBuffer bytes.Buffer
// 	err := m.db.View(func(tx *buntdb.Tx) error {
// 		var ids []string
// 		ct := 0
// 		err := tx.Nearby(Locations, lonLat, func(key, val string, dist float64) bool {
// 			ids = append(ids, extractID(key))
// 			ct++
// 			return ct < numResults
// 		})
// 		if err != nil {
// 			return err
// 		}

// 		for _, id := range ids {
// 			s, err := tx.Get(fmt.Sprintf(NamesPattern, id))
// 			if err != nil {
// 				return err
// 			}
// 			_, err = outBuffer.WriteString(s)
// 			if err != nil {
// 				return err
// 			}
// 			_, err = outBuffer.WriteString("\n")
// 			if err != nil {
// 				return err
// 			}
// 		}

// 		return nil
// 	})
// 	if err != nil {
// 		return "", err
// 	}

// 	return outBuffer.String(), nil
// }
