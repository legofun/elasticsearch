package elasticsearch

import (
	"context"
	"errors"
	"github.com/limitedlee/microservice/common/config"
	"github.com/olivere/elastic/v7"
	"time"
)

var client *elastic.Client

type esClient struct {
	c        *elastic.Client
	ctx      context.Context
	query    elastic.Query
	querys   []elastic.Query
	collapse *elastic.CollapseBuilder
	sortBy   []elastic.Sorter
}

func NewEsClient() (*esClient, error) {
	host := config.GetString("elasticsearch")
	uname := config.GetString("elasticsearch.LoginName")
	passwd := config.GetString("elasticsearch.Password")

	if client != nil {
		_, code, err := client.Ping(host).Do(context.Background())
		//链接已存在直接返回
		if err == nil && code == 200 {
			return &esClient{
				c:   client,
				ctx: context.Background(),
			}, nil
		}
		//log.Printf("Elasticsearch returned with code %d and version %s\n", code)
	}

	//链接不存在，创建
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(host),
		elastic.SetBasicAuth(uname, passwd),
	)
	if err != nil {
		return nil, err
	}

	return &esClient{
		c:   client,
		ctx: context.Background(),
	}, nil
}

//设置超时时间
func (client *esClient) SetTimeout(timeout time.Duration) {
	client.ctx, _ = context.WithTimeout(client.ctx, timeout)
}

//单字段单条件查询
func (client *esClient) SetMatchQuery(name string, values interface{}) {
	client.querys = append(client.querys, elastic.NewMatchQuery(name, values))
}

//单字段多条件查询，类似sql里的in
func (client *esClient) SetTermsQuery(name string, values []interface{}) {
	client.querys = append(client.querys, elastic.NewTermsQuery(name, values...))
}

//不带条件查询，返回全部数据
func (client *esClient) SetMatchAllQuery() {
	client.querys = append(client.querys, elastic.NewMatchAllQuery())
}

//设置折叠条件
func (client *esClient) SetCollapse(innerHitName, field string, size int) {
	innerHit := elastic.NewInnerHit().Name(innerHitName).Size(size)

	client.collapse = elastic.NewCollapseBuilder(field).InnerHit(innerHit)
}

//设置排序条件
func (client *esClient) SetGeoDistanceSort(fieldName, latLon, unit string, ascending bool) {
	pointStr, _ := elastic.GeoPointFromString(latLon)
	sorter := elastic.NewGeoDistanceSort(fieldName).Points(pointStr).Unit(unit).Order(ascending)
	client.sortBy = append(client.sortBy, sorter)
}

//搜索
func (client *esClient) Search(indexName string, pageIndex, pageSize int) (*elastic.SearchResult, error) {
	if pageIndex <= 0 {
		pageIndex = 1
	}
	if pageSize <= 0 {
		pageIndex = 10
	}

	client.query = elastic.NewBoolQuery().Must(client.querys...)
	ss := client.c.Search().Index(indexName).Query(client.query)

	if client.collapse != nil {
		ss = ss.Collapse(client.collapse)
	}
	if len(client.sortBy) > 0 {
		for _, s := range client.sortBy {
			ss = ss.SortBy(s)
		}
	}

	res, err := ss.From(pageIndex - 1).Size(pageSize).Pretty(true).Do(client.ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (client *esClient) Delete(indexName, id string) error {
	doc, err := client.c.Delete().
		Index(indexName).
		Id(id).
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return err
	}
	if doc.Result != "deleted" {
		return errors.New("删除失败")
	}

	return nil
}

func (client *esClient) Bulk(data []elastic.BulkableRequest) error {
	bulkRequest := client.c.Bulk()

	for _, v := range data {
		bulkRequest = bulkRequest.Add(v)
	}

	_, err := bulkRequest.
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return err
	}

	//if bulkRequest.NumberOfActions() == 0 {
	//	glog.Infof("Actions all clear!")
	//}

	return nil
}

func (client *esClient) Save(indexName, id string, data interface{}) error {
	// 写入
	_, err := client.c.Index().
		Index(indexName).
		Id(id).
		BodyJson(data).
		Refresh("wait_for").
		Do(client.ctx)
	if err != nil {
		return err
	}

	return nil
}
