package scheduler

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
)

type Webhook struct {
	Method        string              `json:"method,omitempty"`
	URL           string              `json:"url"`
	Headers       map[string][]string `json:"headers,omitempty"`
	Body          string              `json:"body,omitempty"`
	Base64Encoded bool                `json:"base64_encoded,omitempty"`
}

func (hook *Webhook) Call() {
	method := hook.Method
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	if hook.Body != "" {
		if hook.Base64Encoded {
			bodyData, err := base64.StdEncoding.DecodeString(hook.Body)
			if err  == nil {
				body = bytes.NewReader(bodyData)
			}
		} else {
			body = bytes.NewReader([]byte(hook.Body))
		}
	}
	req, err := http.NewRequest(method, hook.URL, body)
	if err != nil {
		return
	}
	if hook.Headers != nil {
		for key, vals := range hook.Headers {
			for _, val := range vals {
				req.Header.Add(key, val)
			}
		}
	}
	c := &http.Client{}
	res, err := c.Do(req)
	if err == nil {
		buf := make([]byte, 8192)
		for {
			_, err := res.Body.Read(buf)
			if err != nil {
				break
			}
		}
		res.Body.Close()
	}
}
