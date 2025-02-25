package rest

import (
	"go-search/indexer"
	"net/http"

	helper "github.com/rosbit/http-helper"
)

// PUT /doc/:index
//
// add one document to index
//
// POST body:
// {
//   "field-name": "xxx",
//   ...
// }
func IndexDoc(c *helper.Context) {
	updateDoc(c, indexer.IndexDoc, "doc added to index")
}

// PUT /update/:index
//
// update an existing document. there must be pk fields in the body.
//
// POST body:
// {
//   "field-name": "xxx",
//   ...
// }
func UpdateDoc(c *helper.Context) {
	updateDoc(c, indexer.UpdateDoc, "doc updated to index")
}

func updateDoc(c *helper.Context, fnUpdateDoc indexer.FnUpdateDoc, okStr string) {
	index := c.Param("index")

	var doc map[string]interface{}
	if code, err := c.ReadJSON(&doc); err != nil {
		_ = c.Error(code, err.Error())
		return
	}
	docID, err := fnUpdateDoc(index, doc)
	if err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}
	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code": http.StatusOK,
		"msg":  okStr,
		"id":   docID,
	})
}

// PUT /docs/:index[?cb=url-encoded-callback-url]
//
// add 1 or more documents to index
//
// path parameter
//  - index  name of index
// POST Head:
//   - Content-Type: multipart/form-data
//   arguments:
//   - file  file name with ext ".json"/".csv" to upload
//
// ---- OR ----
//
//   - Content-Type: application/json
//   POST body:
//   [
//     {doc 1},
//     {doc 2},
//      ...
//   ]
//
// ---- OR -----
//   - Content-Type: text/csv
//   POST body:
//   field-name1,fn2,fn3,...
//   val1,v2,v3,...
//   val1,v2,v3,...
//
// ---- OR -----
//   - Content-Type: application/x-ndjson
//   POST body:
//   {json}
//   {json}
func IndexDocs(c *helper.Context) {
	index := c.Param("index")

	in, contentType, ext, err := getReader(c, "file")
	if err != nil {
		_ = c.Error(http.StatusNotAcceptable, err.Error())
		return
	}
	defer in.Close()

	var indexReader indexer.FnIndexReader
	var ok bool
	if contentType == MultipartForm {
		if indexReader, ok = ext2Indexer[ext]; !ok {
			indexReader = indexer.IndexJSON
		}
	} else {
		if indexReader, ok = contentType2Indexer[contentType]; !ok {
			indexReader = indexer.IndexJSON
		}
	}

	cb := c.QueryParam("cb")
	if cb == "" {
		docIds, err := indexReader(index, in)
		if err != nil && docIds != nil {
			_ = c.Error(http.StatusInternalServerError, err.Error())
			return
		}
		_ = c.JSON(http.StatusOK, map[string]interface{}{
			"code": http.StatusOK,
			"msg":  "docs added to index",
			"ids":  docIds,
		})
	} else {
		tmpName, inTmp, err := saveTmpFile(in)
		if err != nil {
			_ = c.Error(http.StatusInternalServerError, err.Error())
			return
		}
		indexReader(index, inTmp, cb, tmpName)
		_ = c.JSON(http.StatusOK, map[string]interface{}{
			"code": http.StatusOK,
			"msg":  "indexing request accepted",
		})
	}
}

var ext2Indexer = map[string]indexer.FnIndexReader{
	".csv":   indexer.IndexCSV,
	".jsonl": indexer.IndexJSONLines,
	".json":  indexer.IndexJSON,
}

var contentType2Indexer = map[string]indexer.FnIndexReader{
	JSONMime:      indexer.IndexJSON,
	CsvMime:       indexer.IndexCSV,
	jSONLinesMime: indexer.IndexJSONLines,
}
