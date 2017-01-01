<?php

/**
 * query example:
   GET /_search
    {
      "query": {
        "multi_match": {
          "query": "Search query",
          "fields": [ "keywords", "title", "brand" ],
          "tie_breaker": 0.3
        }
      }
    }
 */


namespace Nocake\Service;

use Elasticsearch\ClientBuilder;
use GuzzleHttp\Client;
use Prewk\XmlStringStreamer;

class AffilinetImporter implements ImporterInterface {
  const SOURCES_URL_TEMPLATE = 'http://publisher.affili.net/Download/AutoDownload.aspx?PartnerID=%d&csvPW=%s';
  const DOWNLOAD_URL_TEMPLATE = 'http://productdata.download.affili.net/affilinet_products_%d_%d.xml?auth=%s';
  const PARTNER_ID = '493114';
  const API_PASSWORD = 'qfLfcZ4GPWVJUATGIyCY';
  const DOWNLOAD_PATH = 'affilinet_importer_download';
  const IGNORE_LISTS = [4745, 3661, 1206, 5915];

  private $lists = [];
  private $http;
  private $indexName;

  /**
   * @var \Elasticsearch\Client
   */
  private $elasticSearch;

  public function __construct() {
    $this->http = new Client();
    $this->elasticSearch = ClientBuilder::create()->build();

    libxml_use_internal_errors(TRUE);
  }

  public function import($download = 1) {
    $this->getSources();
    $this->createIndex();
    try {
      foreach ($this->lists as $list) {
        if (!in_array($list['ListID'], self::IGNORE_LISTS)) {
          $this->log("Importing ${list['Products']} products from '${list['Titel']}' (ListID: ${list['ListID']})", 'info');
          $this->importList($list, $download);
        }
      }
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
      $this->deleteIndex();
      // do not update aliases
      return;
    }

    $this->updateAlias();
  }

  private function getSources() {
    $sourcesUrl = sprintf(
      self::SOURCES_URL_TEMPLATE,
      self::PARTNER_ID,
      self::API_PASSWORD
    );

    $res = $this->http->request('GET', $sourcesUrl);
    if ($res->getStatusCode() != 200) {
      throw new \Exception('Cannot retrieve affilinet catalog sources.');
    }

    $contentType = $res->getHeader('Content-Type')[0];
    if (strpos($contentType, '=') === FALSE) {
      $this->log("No charset found: $contentType", 'warning');
      $charset = '';
    }
    else {
      $charset = explode('=', $contentType)[1];
    }

    $catalogSources = (string) $res->getBody();
    $catalogSources = str_replace('<br>', '', $catalogSources);

    $catalogSources = mb_convert_encoding(
      $catalogSources,
      'UTF-8',
      $charset
    );

    $csv = new \parseCSV();
    $csv->sort_by = 'Products';
    $csv->delimiter = ';';
    $csv->parse($catalogSources);
    if(empty($csv->data)) {
      throw new \Exception('No product lists found..');
    }
    $this->lists = $csv->data;
  }

  private function log($message, $type) {
    echo $type . ': ' . trim($message) . PHP_EOL;
  }

  private function importList($list, $download) {
    if (!$localFile = $this->getProductList($list, $download)) {
      return;
    }
    $shop = 'affilinet-' .  $list['ListID'];

    $streamer = XmlStringStreamer::createStringWalkerParser($localFile);

    while ($node = $streamer->getNode()) {
      $product = simplexml_load_string($node);
      $productInfo = $this->parseProductInfo($product, $shop);
      $this->indexProduct($productInfo, $shop);
    }

  }

  private function createIndex() {
    $indices = $this->elasticSearch->indices();
    $index = $this->getIndexParameter();

    try {
      if ($indices->exists($index)) {
        $this->log("Index ${index['index']} already existed", 'info');
      } else {
        $indices->create($this->getIndexParams());
        $this->log("Index ${index['index']} created", 'info');
      }
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
    }
  }
  private function deleteIndex() {
    $indices = $this->elasticSearch->indices();
    if($indices->exists($this->getIndexParameter())){
      $indices->delete($this->getIndexParameter());
    }
  }

  /**
   * @return array
   */
  private function getBaseParams(): array {
    return $this->getIndexParameter() + [
      'type' => 'product',
    ];
  }

  /**
   * @return array
   */
  private function getIndexParameter(): array {
    if (empty($this->indexName)){
      $this->getNewIndexName();
    }

    return [
      'index' => $this->indexName,
    ];
  }

  private function getNewIndexName() {
    $counter = 1;
    $indexName = 'nocake_' . date('Ymd') . '_';
    $indices = $this->elasticSearch->indices();

    while ($indices->exists(['index' => $indexName . $counter])) {
      $counter++;
    }
    $this->indexName = $indexName . $counter;
  }

  private function getAliasName() {
    return 'nocake';
  }

  private function getAliasParameter() {
    return [
      'name' => $this->getAliasName(),
    ];
  }

  private function updateAlias() {
    $aliasParameter = $this->getAliasParameter();
    $indexParameter = $this->getIndexParameter();

    $indices = $this->elasticSearch->indices();

    if ($indices->existsAlias($aliasParameter)) {
      $alias = $indices->getAlias($aliasParameter);
      $index = key($alias);

      $indices->deleteAlias(['index' => $index] + $aliasParameter);
      $this->log("Deleted alias ${aliasParameter['name']}.", 'info');
      $indices->delete(['index' => $index]);
      $this->log("Deleted index ${index}.", 'info');
    }
    $putParameter = $indexParameter + $aliasParameter;
    $indices->putAlias($putParameter);
    $this->log("Created alias ${aliasParameter['name']} for index ${indexParameter['index']}.", 'info');
  }

  /**
   * @return array
   */
  private function getIndexParams(): array {
    $baseParams = $this->getBaseParams();

    /*
     * Using one shard without replicas on single node cluster.
     * Since we do not need good write performance (we only write on imports)
     * we then can add new nodes just by adding more replicas.
     */
    return [
      'index' => $baseParams['index'],
      'body' => [
        'settings' => [
          'number_of_shards' => 1,
          'number_of_replicas' => 0,
          'analysis' => [
            'analyzer' => [
                  'lower_keyword' => [
                    'type' => 'custom',
                    'tokenizer' => 'keyword',
                    'filter' => 'lowercase'
                ]
            ]
          ]
        ],
        'mappings' => [
          $baseParams['type'] => [
            '_source' => [
              'enabled' => true
            ],
            'dynamic' => false,
            '_all' => [
              'enabled' => false
            ],
            'properties' => [
              'list' => [
                'type' => 'keyword',
              ],
              'price' => [
                'type' => 'nested',
                'properties' => [
                  'value' => [
                    'type' => 'scaled_float',
                    'scaling_factor' => 100
                  ],
                  'currency' => [
                    'type' => 'keyword',
                  ],
                ]
              ],
              'articlenumber' => [
                'type' => 'keyword',
              ],
              'link' => [
                'enabled' => false,
              ],
              'title' => [
                'type' => 'text',
                'analyzer' => 'german'
              ],
              'description' => [
                'type' => 'text',
                'analyzer' => 'german'
              ],
              'keywords' => [
                'type' => 'text'
              ],
              'brand' => [
                'type' => 'text',
                'analyzer' => 'lower_keyword'
              ],
              'image' => [
                'enabled' => false,
              ]
            ]
          ]
        ]
      ]
    ];

  }
  private function parseProductInfo($product, $list) : array {
    $displayPrice = explode(' ', (string) $product->Price->DisplayPrice);
    switch(count($displayPrice)) {
      case 0:
        $value = 0;
        $currency = '';
        break;
      case 1:
        $value = (integer) $displayPrice[0];
        $currency = '';
        break;
      default:
        $currency = (string) array_pop($displayPrice);
        $value = (integer) array_pop($displayPrice);
    }


    $keywords = explode(',', (string) $product->Details->Keywords);
    $keywords = array_map('trim', $keywords);

    $indexData = [];
    $indexData['list'] = $list;
    $indexData['price']['value'] = $value;
    $indexData['price']['currency'] = $currency;
    $indexData['articlenumber'] = (string) $product['ArticleNumber'];
    $indexData['link'] = (string) $product->Deeplinks->Product;
    $indexData['title'] = (string) $product->Details->Title;
    $indexData['description'] = (string) $product->Details->DescriptionShort;
    $indexData['keywords'] = $keywords;
    $indexData['brand'] = (string) $product->Details->Brand;
    $indexData['image'] = (string) $product->Images->Img->URL;

    return $indexData;
  }

  private function indexProduct($productInfo, $list) {
    $elasticSearchIndexParams = $this->getBaseParams();
    $elasticSearchIndexParams['id'] = $list . '-' . $productInfo['articlenumber'];
    $elasticSearchIndexParams['body'] = $productInfo;
    try{
        $this->elasticSearch->index($elasticSearchIndexParams);
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
    }
  }

  /**
   * Download a remote product list.
   *
   * @param $list
   *
   * @return bool|string $localFile
   */
  private function getProductList($list, $download) {
    $listDownloadUrl = sprintf(
      self::DOWNLOAD_URL_TEMPLATE,
      $list['ListID'],
      self::PARTNER_ID,
      self::API_PASSWORD
    );

    $urlPath = parse_url($listDownloadUrl)['path'];
    $localTmpPath = sys_get_temp_dir() . '/' . self::DOWNLOAD_PATH;
    $localFile = $localTmpPath . '/' . basename($urlPath);

    if($download || !file_exists($localFile)) {
      if (!file_exists($localTmpPath)) {
        if (!mkdir($localTmpPath, 0777, TRUE)) {
          $this->log('Could not create download directory ' . $localTmpPath, 'error');
          exit();
        }
      }


      $res = $this->http->request('GET', $listDownloadUrl, ['sink' => $localFile]);
      if ($res->getStatusCode() != 200) {
        // something is wrong with this url
        return FALSE;
      }
      $this->log("Downloaded list to $localFile", 'info');
    }
    return $localFile;
  }
}
