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
	c              *elastic.Client
	ctx            context.Context
	querys         []elastic.Query
	orQuerys       []elastic.Query
	sortBy         []elastic.Sorter
	collapse       *elastic.CollapseBuilder
	collapseSortBy []elastic.Sorter
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

//设置查询权重
func (client *esClient) SetOrQuery(name string, values interface{}, args ...interface{}) {
	q := elastic.NewMatchQuery(name, values)
	if len(args) > 0 {
		q = q.Boost(args[0].(float64))
	}
	client.orQuerys = append(client.orQuerys, q)
}

//设置折叠条件
func (client *esClient) SetCollapse(innerHitName, field string, size int) {
	innerHit := elastic.NewInnerHit().Name(innerHitName).Size(size)
	if len(client.collapseSortBy) > 0 {
		innerHit.SortBy(client.collapseSortBy...)
	}

	client.collapse = elastic.NewCollapseBuilder(field).InnerHit(innerHit)
}

//设置折叠排序条件
func (client *esClient) SetCollapseSortBy(field string, ascending bool) {
	sorter := elastic.NewFieldSort(field)
	if ascending {
		sorter = sorter.Asc()
	} else {
		sorter = sorter.Desc()
	}

	client.collapseSortBy = append(client.collapseSortBy, sorter)
}

//设置排序条件
func (client *esClient) SetSortBy(field string, ascending bool) {
	sorter := elastic.NewFieldSort(field)
	if ascending {
		sorter = sorter.Asc()
	} else {
		sorter = sorter.Desc()
	}

	client.sortBy = append(client.sortBy, sorter)
}

//设置geo距离排序条件
//latLon 坐标经纬度，纬度在前
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

	boolQuery := elastic.NewBoolQuery()
	if len(client.querys) > 0 {
		boolQuery = elastic.NewBoolQuery().Must(client.querys...)
	}
	if len(client.orQuerys) > 0 {
		boolQuery = boolQuery.Should(client.orQuerys...)
	}

	ss := client.c.Search().Index(indexName).Query(boolQuery)

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

//删除
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

//批量操作
func (client *esClient) Bulk(data []elastic.BulkableRequest) error {
	if len(data) == 0 {
		return errors.New("no bulk data")
	}

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

//保存
func (client *esClient) Save(indexName, id string, data interface{}) error {
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
