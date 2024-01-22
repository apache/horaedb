package operation

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
)

func tableWriter(headers []string) table.Writer {
	header := table.Row{}
	for _, s := range headers {
		header = append(header, s)
	}
	t := table.NewWriter()
	t.AppendHeader(header)
	return t
}

func HttpUtil(method, url string, body io.Reader, response interface{}) error {
	request, _ := http.NewRequest(method, url, body)
	resp, err := (&http.Client{}).Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, &response)
	return err
}

func FormatTimeMilli(milli int64) string {
	return time.UnixMilli(milli).Format("2006-01-02 15:04:05.000")
}
