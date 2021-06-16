package rest

import (
	"go-search/indexer"
	"net/http"

	helper "github.com/rosbit/http-helper"
)

// DELETE /doc/:index
//
// POST body:
// {
// 	  "id": "string"|integer|other-type,
// }
func DeleteDoc(c *helper.Context) {
	index := c.Param("index")
	var doc struct {
		ID interface{} `json:"id"`
	}
	if code, err := c.ReadJSON(&doc); err != nil {
		_ = c.Error(code, err.Error())
		return
	}
	if err := indexer.DeleteDoc(index, doc.ID); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}

	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code": http.StatusOK,
		"msg":  "doc removed from index",
		"id":   doc.ID,
	})
}

// DELETE /docs/:index
//
// POST body:
// [
// 	  docId1, docId2, ...
// ]
func DeleteDocs(c *helper.Context) {
	index := c.Param("index")

	var docIds []interface{}
	if code, err := c.ReadJSON(&docIds); err != nil {
		_ = c.Error(code, err.Error())
		return
	}

	if err := indexer.DeleteDocs(index, docIds); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}

	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code": http.StatusOK,
		"msg":  "docs removed from index",
		"ids":  docIds,
	})
}
