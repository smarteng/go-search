package rest

import (
	"fmt"
	"go-search/conf"
	"go-search/indexer"
	"net/http"

	helper "github.com/rosbit/http-helper"
)

// POST /schema/:index
//
// create a schema for index
//
// path parameter
//  - index  name of index
// POST Head:
//   - Content-Type: multipart/form-data
//   arguments:
//   - file  file name and content to upload
// ---- OR ----
//   - Content-Type: application/json
//   post body:
//   {schema-json-content}
func CreateSchema(c *helper.Context) {
	if !indexer.IsRunning() {
		_ = c.Error(http.StatusInternalServerError, "service is stopped")
		return
	}
	index := c.Param("index")
	if _, err := conf.LoadSchema(index); err == nil {
		errStr := fmt.Sprintf("schema of index %s exists already, please remove it first", index)
		_ = c.Error(http.StatusInternalServerError, errStr)
		return
	}

	jsonFile, _, _, err := getReader(c, "file")
	if err != nil {
		_ = c.Error(http.StatusBadRequest, err.Error())
		return
	}
	defer jsonFile.Close()

	if err := conf.SaveSchema(index, jsonFile); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}
	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code":  http.StatusOK,
		"msg":   "schema created",
		"index": index,
	})
}

// DELETE /schema/:index
//
// delete the schema file and all the stored index files.
//
// path parameter
//  - index  name of index
func DeleteSchema(c *helper.Context) {
	index := c.Param("index")

	indexer.RemoveIndexer(index)
	if err := conf.DeleteSchema(index); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}

	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code":  http.StatusOK,
		"msg":   "schema deleted",
		"index": index,
	})
}

// GET /schema/:index
//
// show schema file content
//
// path parameter
//  - index  name of index
func ShowSchema(c *helper.Context) {
	index := c.Param("index")
	if schema, err := conf.LoadSchema(index); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
	} else {
		_ = c.JSON(http.StatusOK, schema.SchemaConf)
	}
}

// PUT /schema/:index/:newIndex
//
// Rename schema name
//
// path parameter
//  - index     name of existing index
//  - newIndex  new name of the index
func RenameSchema(c *helper.Context) {
	index := c.Param("index")
	newIndex := c.Param("newIndex")
	if _, err := conf.LoadSchema(index); err != nil {
		_ = c.Error(http.StatusNotFound, fmt.Sprintf("index %s not found", index))
		return
	}
	if _, err := conf.LoadSchema(newIndex); err == nil {
		_ = c.Error(http.StatusInternalServerError, fmt.Sprintf("index %s alreday exists", newIndex))
		return
	}

	indexer.LruRemove(index)
	indexer.RemoveIndexer(index)

	if err := conf.RenameSchema(index, newIndex); err != nil {
		_ = c.Error(http.StatusInternalServerError, err.Error())
		return
	}

	_ = c.JSON(http.StatusOK, map[string]interface{}{
		"code": http.StatusOK,
		"msg":  fmt.Sprintf("index %s renamed to %s OK", index, newIndex),
	})
}
