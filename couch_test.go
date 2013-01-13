package couch

import (
	"bufio"
	"bytes"
	"net/http"
	"net/url"
	"testing"
)

var couchURL1 = "https://user:pass@nvlope.cloudant.com:1234/mail"
var couchURL2 = "http://user2:pass2@nvlope2.cloudant.com/mydb"

func TestNewCouch(t *testing.T) {
	couch, err := NewCouch("")
	if err == nil {
		t.Fatal("error nil")
	}
	couch, err = NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch == nil {
		t.Fatal("couch is nil")
	}
	if couch.url == nil {
		t.Fatal("couch url is nil")
	}
	if couch.send == nil {
		t.Fatal("couch send func is nil")
	}
	couch, err = NewCouch(couchURL2)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch == nil {
		t.Fatal("couch is nil")
	}
	if couch.url == nil {
		t.Fatal("couch url is nil")
	}
	if couch.send == nil {
		t.Fatal("couch send func is nil")
	}
}

func TestSecure(t *testing.T) {
	couch := &Couch{}
	if couch.Secure() {
		t.Fatal("should return false on uninitialized struct")
	}
	couch, err := NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if !couch.Secure() {
		t.Fatal("couch not secure")
	}
	couch, err = NewCouch(couchURL2)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch.Secure() {
		t.Fatal("couch secure")
	}
}

func TestDb(t *testing.T) {
	couch := &Couch{}
	if couch.Db() != "" {
		t.Fatal("should be empty on uninitialized struct")
	}
	couch, err := NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch.Db() != "mail" {
		t.Fatal("expected 'mail':", couch.Db())
	}
	couch, err = NewCouch(couchURL2)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch.Db() != "mydb" {
		t.Fatal("expected 'mydb':", couch.Db())
	}
}

func TestBaseURL(t *testing.T) {
	couch := &Couch{}
	if couch.BaseURL() != "" {
		t.Fatal("should be empty on uninitialized struct")
	}
	couch, err := NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch.BaseURL() != "https://nvlope.cloudant.com:1234" {
		t.Fatal("base url wrong", couch.BaseURL())
	}
}

func TestAllDbsURL(t *testing.T) {
	couch := &Couch{}
	if couch.AllDbsURL() != "" {
		t.Fatal("should be empty on uninitialized struct")
	}
	couch, err := NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if couch.AllDbsURL() != "https://nvlope.cloudant.com:1234/_all_dbs" {
		t.Fatal("all dbs url wrong", couch.AllDbsURL())
	}
}

func TestReq(t *testing.T) {
	couch := &Couch{}
	recovered := false
	func() {
		defer func() { recovered = (recover() != nil) }()
		couch.req("method", "url", nil, nil, nil)
	}()
	if !recovered {
		t.Fatal("should have recovered")
	}
	couch, err := NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	couch.send = func(req *http.Request) (*http.Response, error) {
		buf := bytes.NewBufferString("")
		req.Write(buf)
		expect := "POST / HTTP/1.1\r\n" +
			"Host: google.com\r\n" +
			"User-Agent: Go http package\r\n" +
			"Connection: close\r\n" +
			"Transfer-Encoding: chunked\r\n" +
			"Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=\r\n" +
			"X-Test: x-test-value\r\n" +
			"\r\n" +
			"4\r\n" +
			"body\r\n" +
			"0\r\n\r\n"
		if buf.String() != expect {
			t.Fatal("not equal", buf.String(), expect)
		}
		return nil, nil
	}
	couch.req(
		"POST",
		"http://google.com",
		map[string][]string{
			"X-Test": []string{"x-test-value"},
		},
		[]byte("body"),
		url.UserPassword("username", "password"),
	)
}

func makeSendFunc(s string, method string) func(req *http.Request) (*http.Response, error) {
	r := bufio.NewReader(bytes.NewBufferString(s))
	resp, err := http.ReadResponse(r, &http.Request{Method: method})
	if err != nil {
		panic(err)
	}
	return func(req *http.Request) (*http.Response, error) {
		return resp, nil
	}
}

func TestRunning(t *testing.T) {
	couch := &Couch{}
	ok, err := couch.Running()
	if err == nil {
		t.Fatal("error nil")
	}
	if ok {
		t.Fatal("should be false on uninitialized struct")
	}
	couch, err = NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	expectedHeaders := "HTTP/1.1 200 OK\r\n" +
		"Content-Length: 63\r\n" +
		"Cache-Control: must-revalidate\r\n" +
		"Content-Type: text/plain;charset=utf-8\r\n" +
		"Date: Fri, 23 Nov 2012 16:28:47 GMT\r\n" +
		"Server: CouchDB/1.0.2 (Erlang OTP/R14B)\r\n" +
		"X-Couch-Request-Id: 103a230d\r\n\r\n"
	resp1 := expectedHeaders + "{\"version\":\"1.0.2\",\"cloudant_build\":\"836\"}\r\n\r\n"
	resp2 := expectedHeaders + "{\"couchdb\":\"Welcome\",\"version\":\"1.0.2\",\"cloudant_build\":\"836\"}\r\n\r\n"
	couch.send = makeSendFunc(resp1, "GET")
	ok, err = couch.Running()
	if err != nil {
		t.Fatal("err not nil", err)
	}
	if ok {
		t.Fatal("should not be running")
	}
	couch.send = makeSendFunc(resp2, "GET")
	ok, err = couch.Running()
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if !ok {
		t.Fatal("should be running")
	}
}

func TestInsert(t *testing.T) {
	var MyObj struct {
		Field1 string
		Field2 int
	}
	couch := &Couch{}
	id, rev, err := couch.Insert(MyObj)
	if err == nil {
		t.Fatal("error nil")
	}
	if id != "" || rev != "" {
		t.Fatal("should be empty on uninitialized struct")
	}
	couch, err = NewCouch(couchURL1)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	respWire := "HTTP/1.1 201 Created\r\n" +
		"Connection: close\r\n" +
		"Content-Length: 95\r\n" +
		"Cache-Control: must-revalidate\r\n" +
		"Content-Type: text/plain;charset=utf-8\r\n" +
		"Date: Fri, 23 Nov 2012 19:31:59 GMT\r\n" +
		"Location: http://nvlope.cloudant.com/mail/c37a4626aa8e874f2df7ae1534a96587\r\n" +
		"Server: CouchDB/1.0.2 (Erlang OTP/R14B)\r\n" +
		"X-Couch-Request-Id: 1b747b90\r\n\r\n" +
		"{\"ok\":true,\"id\":\"c37a4626aa8e874f2df7ae1534a96587\",\"rev\":\"1-31cf9ceb7c18cfa7e77367760268af3b\"}\r\n\r\n"
	couch.send = makeSendFunc(respWire, "POST")
	id, rev, err = couch.Insert(MyObj)
	if err != nil {
		t.Fatal("error not nil", err)
	}
	if id != "c37a4626aa8e874f2df7ae1534a96587" {
		t.Fatal("invalid id", id)
	}
	if rev != "1-31cf9ceb7c18cfa7e77367760268af3b" {
		t.Fatal("invalid rev", rev)
	}
}

func TestQuery(t *testing.T) {
	// TODO: implement
}
