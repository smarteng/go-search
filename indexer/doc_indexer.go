package indexer

import (
	"fmt"
	"go-search/conf"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/go-ego/riot/types"
	"github.com/rosbit/go-wget"
)

// IndexJSON/IndexCSV/... 等从文件获取doc建索引的函数签名
type FnIndexReader func(string, io.ReadCloser, ...string) ([]string, error)

// IndexDoc/UpdateDoc: 更新一个doc
type FnUpdateDoc func(index string, doc map[string]interface{}) (docId string, err error)

// 把一个doc添加到索引库
func IndexDoc(index string, doc map[string]interface{}) (docID string, err error) {
	if !running {
		return "", fmt.Errorf("the service is stopped")
	}

	idx, err := initIndexer(index)
	if err != nil {
		return "", fmt.Errorf("schema %s not found, please create schema first", index)
	}

	docID, err = idx.indexDoc(doc)
	if err != nil {
		return "", err
	}
	idx.flush()
	return docID, nil
}

// 更新一个doc，可以只更新出现的字段。如果doc不存在，更新会失败
func UpdateDoc(index string, doc map[string]interface{}) (docID string, err error) {
	if !running {
		return "", fmt.Errorf("the service is stopped")
	}

	idx, err := initIndexer(index)
	if err != nil {
		return "", fmt.Errorf("schema %s not found, please create schema first", index)
	}

	existingDoc, err := idx.getDoc(doc)
	if err != nil {
		return "", err
	}
	fmt.Printf("exsiting doc: %v\n", existingDoc)
	fmt.Printf("updating doc: %v\n", doc)

	for k, v := range doc {
		existingDoc[k] = v
	}
	fmt.Printf("new doc: %v\n", existingDoc)

	docID, err = idx.indexDoc(existingDoc)
	if err != nil {
		return "", err
	}
	idx.flush()
	return docID, nil
}

// 把多个JSON(JSON数组)添加到索引库
func IndexJSON(index string, in io.ReadCloser, cb ...string) (docIds []string, err error) {
	return indexFromDocGenerator(index, in, fromJSONFile, cb...)
}

// 把csv中的一行作为doc添加到索引库
func IndexCSV(index string, in io.ReadCloser, cb ...string) (docIds []string, err error) {
	return indexFromDocGenerator(index, in, fromCsvFile, cb...)
}

// 把JSON Lines(每行一个JSON)添加到索引库
func IndexJSONLines(index string, in io.ReadCloser, cb ...string) (docIds []string, err error) {
	return indexFromDocGenerator(index, in, fromJSONLines, cb...)
}

//从文件获取doc做索引的统一流程，不同的文件类型需要实现一个fnReaderGenerator
func indexFromDocGenerator(
	index string,
	in io.ReadCloser,
	docGenerator fnReaderGenerator, cb ...string,
) (docIds []string, err error) {
	var idx *indexer
	var docChan <-chan Doc

	if !running {
		err = fmt.Errorf("the service is stopped")
		goto ERROR
	}

	idx, err = initIndexer(index)
	if err != nil {
		err = fmt.Errorf("schema %s not found, please create schema first", index)
		goto ERROR
	}

	docChan, err = docGenerator(in)
	if err != nil {
		goto ERROR
	}
	if len(cb) == 0 {
		defer in.Close()
		// no callback
		return idx.indexDocs(docChan), nil
	}

	// with callback
	go func() {
		defer os.Remove(cb[1])
		defer in.Close()
		idx.indexDocs(docChan, cb...)
	}()
	return nil, nil

ERROR:
	in.Close()
	if len(cb) > 0 {
		os.Remove(cb[1])
	}
	return
}

// 删除一个doc
func DeleteDoc(index string, docID interface{}) error {
	if !running {
		return fmt.Errorf("the service is stopped")
	}
	idx, err := initIndexer(index)
	if err != nil {
		return fmt.Errorf("schema %s not found, please create schema first", index)
	}
	idx.deleteDoc(fmt.Sprintf("%v", docID))
	idx.flush()
	return nil
}

// 删除多个doc
func DeleteDocs(index string, docIds []interface{}) error {
	if !running {
		return fmt.Errorf("the service is stopped")
	}

	idx, err := initIndexer(index)
	if err != nil {
		return fmt.Errorf("schema %s not found, please create schema first", index)
	}
	for _, docID := range docIds {
		idx.deleteDoc(fmt.Sprintf("%v", docID))
	}
	idx.flush()
	return nil
}

//索引中增加一个文档
func (idx *indexer) indexDoc(doc map[string]interface{}) (string, error) {
	storedDoc := StoredDoc{}
	tokens := []types.TokenData{}

	fm := idx.schema.FieldMap
	fields := idx.schema.Fields
	engine := idx.engine
	startLoc := 0
	pk := map[int]interface{}{}
	for fieldName, value := range doc {
		fieldIdx, ok := fm[fieldName]
		if !ok {
			continue
		}
		field := &fields[fieldIdx]

		val, err := field.ToNativeValue(value)
		if err != nil {
			return "", err
		}
		if field.PK {
			pk[fieldIdx] = val
		}

		switch val.(type) {
		case string:
			s := val.(string)
			var segTokens []string
			switch field.Tokenizer {
			case conf.ZH_TOKENIZER:
				// segTokens = engine.Segment(s)
				segTokens = hanziTokenize(s)
			case conf.NONE_TOKENIZER:
				// segTokens = []string{strings.TrimSpace(s)}
				val = strings.TrimSpace(s)
			default:
				segTokens = whitespaceTokenize(s)
			}
			if len(segTokens) > 0 {
				fieldTokens := buildIndexTokens(fieldIdx, segTokens, startLoc)
				tokens = append(tokens, fieldTokens...)
				startLoc += len(fieldTokens) + 10 // 与下一字段的索引间加上几个间隔
			}
		default:
		}

		storedDoc[fieldName] = val
	}
	pkIdx := idx.schema.PKIdx
	if len(pk) != len(pkIdx) {
		return "", fmt.Errorf("pk field must be specified")
	}

	docID := strings.Builder{}
	for i, idx := range pkIdx {
		if i > 0 {
			docID.WriteByte('_')
		}
		docID.WriteString(fmt.Sprintf("%v", pk[idx]))
	}

	dID := docID.String()
	count := mergeTokenLocs(&tokens)
	indexerChan <- &indexerOp{
		op:     _INDEX_DOC,
		engine: engine,
		docID:  dID,
		doc: &types.DocData{
			Tokens: tokens[:count],
			Fields: storedDoc,
			Labels: allDocs,
		},
	}
	return dID, nil
}

//批量增加索引文档
func (idx *indexer) indexDocs(docs <-chan Doc, cb ...string) (docIds []string) {
	hasError := false
	hasCb := len(cb) > 0

	count := 0
	for doc := range docs {
		if doc.err != nil {
			if !hasCb {
				docIds = append(docIds, doc.err.Error())
			} else {
				log.Printf("[error] indexing %s: %v\n", idx.schema.Name, doc.err.Error())
			}
			continue
		}

		if docID, err := idx.indexDoc(doc.doc); err != nil {
			if !hasCb {
				docIds = append(docIds, err.Error())
			} else {
				log.Printf("[error] indexing %s: %v\n", idx.schema.Name, err.Error())
			}
			hasError = true
		} else {
			if !hasCb {
				docIds = append(docIds, docID)
			}
			count++
		}
	}

	if count > 0 {
		idx.flush()
	}
	log.Printf("[info] %d docs appended to index %s\n", count, idx.schema.Name)

	if hasCb {
		params := func() map[string]interface{} {
			if hasError {
				return map[string]interface{}{
					"code":  http.StatusInternalServerError,
					"msg":   "failed to index docs",
					"index": idx.schema.Name,
					"docs":  count,
				}
			}
			return map[string]interface{}{
				"code":  http.StatusOK,
				"msg":   "OK",
				"index": idx.schema.Name,
				"docs":  count,
			}
		}

		status, content, _, err := wget.PostJson(cb[0], "POST", params(), nil)
		if err != nil {
			log.Printf("failed to send callback to %s: %d\n", cb[0], status)
		} else {
			log.Printf("send to callback to %s OK: %s\n", cb[0], string(content))
		}
	}
	return
}

//给每个token加上位置信息，同时生成某个字段内的索引
func buildIndexTokens(fieldIdx int, tokens []string, startLoc int) []types.TokenData {
	j := len(tokens)
	res := make([]types.TokenData, j*2)
	for i, token := range tokens {
		res[i] = types.TokenData{
			Text:      token,
			Locations: []int{startLoc + i},
		}

		res[j] = types.TokenData{
			Text:      fmt.Sprintf("f%d:%s", fieldIdx, token),
			Locations: []int{startLoc + j},
		}
		j++
	}

	return res
}

func mergeTokenLocs(pTokens *[]types.TokenData) int {
	tokens := *pTokens
	c := len(tokens)
	pos := make(map[string]int, c) // token -> idx in tokens
	count := 0

	for i := 0; i < c; i++ {
		token := &tokens[i]
		if idx, ok := pos[token.Text]; !ok {
			pos[token.Text] = count
			if count != i {
				tokens[count] = *token
			}
			count++
		} else {
			mToken := &tokens[idx]
			mToken.Locations = append(mToken.Locations, token.Locations...)
		}
	}
	return count
}

func (idx *indexer) deleteDoc(docID string) {
	indexerChan <- &indexerOp{
		op:     _DELETE_DOC,
		engine: idx.engine,
		docID:  docID,
	}
}

func (idx *indexer) flush() {
	indexerChan <- &indexerOp{
		op:     _FLUSH_DOC,
		engine: idx.engine,
	}
}
