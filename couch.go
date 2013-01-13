package couch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

type (
	Id  string
	Rev string
)

type Row struct {
	Id    Id
	Key   interface{}
	Value interface{}
}

type Result struct {
	Rows      []*Row
	TotalRows uint64
	Offset    uint64
}

const (
	PKey           = "key"            // Must be a proper URL encoded JSON value
	PKeys          = "keys"           // Must be a proper URL encoded JSON array value
	PStartKey      = "startkey"       // Must be a proper URL encoded JSON value
	PStartKeyDocID = "startkey_docid" // document id to start with (to allow pagination for duplicate startkeys)
	PEndKey        = "endkey"         // Must be a proper URL encoded JSON value
	PEndKeyDocID   = "endkey_docid"   // last document id to include in the output (to allow pagination for duplicate endkeys)
	PLimit         = "limit"          // Limit the number of documents in the output
	PStale         = "stale"          // If stale=ok is set, CouchDB will not refresh the view even if it is stale, the benefit is a an improved query latency. If stale=update_after is set, CouchDB will update the view after the stale result is returned. update_after was added in version 1.1.0.
	PDescending    = "descending"     // change the direction of search
	PSkip          = "skip"           // skip n number of documents
	PGroup         = "group"          // The group option controls whether the reduce function reduces to a set of distinct keys or to a single result row.
	PGroupLevel    = "group_level"    // 
	PReduce        = "reduce"         // use the reduce function of the view. It defaults to true, if a reduce function is defined and to false otherwise.
	PIncludeDocs   = "include_docs"   // automatically fetch and include the document which emitted each view entry
	PInclusiveEnd  = "inclusive_end"  // Controls whether the endkey is included in the result. It defaults to true.
	PUpdateSeq     = "update_seq"     // Response includes an update_seq value indicating which sequence id of the database the view reflects
)

type Couch struct {
	url  *url.URL
	send func(req *http.Request) (*http.Response, error)
}

func NewCouch(rawurl string) (*Couch, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	return &Couch{
		url: u,
		send: func(req *http.Request) (*http.Response, error) {
			return http.DefaultClient.Do(req)
		},
	}, nil
}

func (c *Couch) Secure() bool {
	if c.url != nil {
		return c.url.Scheme == "https"
	}
	return false
}

func (c *Couch) Db() string {
	db := ""
	if c.url != nil {
		if strings.HasPrefix(c.url.Path, "/") {
			db = c.url.Path[1:]
		}
	}
	return db
}

func (c *Couch) BaseURL() string {
	if c.url != nil {
		return c.url.Scheme + "://" + c.url.Host
	}
	return ""
}

func (c *Couch) AllDbsURL() string {
	base := c.BaseURL()
	if base != "" {
		return c.BaseURL() + "/_all_dbs"
	}
	return ""
}

func (c *Couch) req(method, url string, headers http.Header, body []byte, user *url.Userinfo) (*http.Response, error) {
	if c.send == nil {
		panic("send func not set")
	}
	// Create a new request
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	req.Close = true
	req.TransferEncoding = []string{"chunked"}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	req.ContentLength = int64(len(body))

	// Set headers
	if headers != nil {
		req.Header = headers
	}

	// Set auth credentials
	if user != nil {
		if p, ok := user.Password(); ok {
			req.SetBasicAuth(user.Username(), p)
		}
	}

	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *Couch) Running() (bool, error) {
	baseURL := c.BaseURL()
	if baseURL == "" {
		return false, fmt.Errorf("couch url not valid")
	}
	resp, err := c.req("GET", baseURL, nil, nil, c.url.User)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("returned invalid status %d", resp.StatusCode)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	var d map[string]string
	err = json.Unmarshal(b, &d)
	if err != nil {
		return false, err
	}
	return (d["version"] != "" && d["couchdb"] == "Welcome"), nil
}

func verifyAndUnmarshalResponse(resp *http.Response, status int) (map[string]interface{}, error) {
	if resp.StatusCode != status {
		return nil, fmt.Errorf("returned invalid status %d (expected %d)", resp.StatusCode, status)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var v map[string]interface{}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c *Couch) Insert(obj interface{}) (Id, Rev, error) {
	baseURL := c.BaseURL()
	db := c.Db()
	if baseURL == "" || db == "" {
		return "", "", fmt.Errorf("couch url not valid")
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return "", "", err
	}
	resp, err := c.req(
		"POST",
		baseURL+"/"+db,
		http.Header{"Content-Type": []string{"application/json"}},
		body,
		c.url.User,
	)
	if err != nil {
		return "", "", err
	}
	v, err := verifyAndUnmarshalResponse(resp, 201)
	if err != nil {
		return "", "", err
	}
	if _, ok := v["id"]; !ok {
		return "", "", fmt.Errorf("id not set")
	}
	if _, ok := v["rev"]; !ok {
		return "", "", fmt.Errorf("rev not set")
	}
	if x, ok := v["ok"]; !ok || x != true {
		return "", "", fmt.Errorf("ok flag not true")
	}
	return Id(v["id"].(string)), Rev(v["rev"].(string)), nil
}

func (c *Couch) Query(path string, bodyJson map[string]interface{}, queryPairs ...interface{}) (*Result, error) {
	var body []byte
	if bodyJson != nil {
		b, err := json.Marshal(bodyJson)
		if err != nil {
			return nil, err
		}
		body = b
	}
	pairs := make([]string, 0, len(queryPairs)/2)
	for i := 0; i < len(queryPairs)-1; i += 2 {
		if k, ok := queryPairs[i].(string); ok {
			v, err := json.Marshal(queryPairs[i+1])
			if err == nil {
				pairs = append(pairs, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(string(v))))
			} else {
				return nil, err
			}
		}
	}
	query := strings.Join(pairs, "&")
	url := c.BaseURL() + "/" + c.Db() + "/" + path + "?" + query
	method := "GET"
	if body != nil {
		method = "POST"
	}
	resp, err := c.req(
		method,
		url,
		http.Header{"Content-Type": []string{"application/json"}},
		body,
		c.url.User,
	)
	if err != nil {
		return nil, err
	}
	respObj, err := verifyAndUnmarshalResponse(resp, 200)
	if err != nil {
		return nil, err
	}
	result := &Result{}
	if x, ok := respObj["total_rows"]; ok {
		if y, ok := x.(float64); ok {
			result.TotalRows = uint64(y)
		} else {
			return nil, fmt.Errorf("invalid total rows value")
		}
	} else {
		return nil, fmt.Errorf("total rows not set")
	}
	if x, ok := respObj["offset"]; ok {
		if y, ok := x.(float64); ok {
			result.Offset = uint64(y)
		} else {
			return nil, fmt.Errorf("invalid offset value")
		}
	} else {
		return nil, fmt.Errorf("offset not set")
	}
	if x, ok := respObj["rows"]; ok {
		if y, ok := x.([]interface{}); ok {
			result.Rows = make([]*Row, 0, 50)
			for _, rowI := range y {
				if row, ok := rowI.(map[string]interface{}); ok {
					if id, ok := row["id"].(string); ok {
						result.Rows = append(result.Rows, &Row{
							Id:    Id(id),
							Key:   row["key"],
							Value: row["value"],
						})
					}
				}
			}
		} else {
			return nil, fmt.Errorf("invalid rows value")
		}
	} else {
		return nil, fmt.Errorf("rows not set")
	}
	return result, nil
}
