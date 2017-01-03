<?php
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
  const BATCH_SIZE = 2500;

  private $lists = [];
  private $http;
  private $indexName;
  private $batch = ['counter' => 0, 'params' => ['body' => []]];

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
          $this->log("Importing ${list['Products']} products from list ${list['ListID']}: '${list['Titel']}'", 'info');
          $this->batchImportList($list, $download);
        }
      }
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
      $this->cleanupIndices();
      return;
    } finally {
      $this->log("Finish Batch", 'info');
      $this->finishBatch();
    }

    $this->updateAlias();
    $this->cleanupIndices();
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
    if (empty($csv->data)) {
      throw new \Exception('No product lists found..');
    }
    $this->lists = $csv->data;
  }

  private function log($message, $type) {
    echo $type . ': ' . trim($message) . PHP_EOL;
  }

  private function createIndex() {
    $indices = $this->elasticSearch->indices();
    $index = $this->getIndexParameter();

    try {
      if ($indices->exists($index)) {
        $this->log("Index ${index['index']} already existed", 'info');
      }
      else {
        $indices->create($this->getIndexingParameters());
        $this->log("Index ${index['index']} created", 'info');
      }
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
    }
  }

  /**
   * @return array
   */
  private function getIndexingParameters(): array {
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
              'enabled' => TRUE
            ],
            'dynamic' => FALSE,
            '_all' => [
              'enabled' => FALSE
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
                'enabled' => FALSE,
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
                'type' => 'text',
                'analyzer' => 'lower_keyword',
                'fielddata' => true
              ],
              'categories' => [
                'type' => 'text',
                'analyzer' => 'lower_keyword',
                'fielddata' => true
              ],
              'brand' => [
                'type' => 'text',
                'analyzer' => 'lower_keyword'
              ],
              'image' => [
                'enabled' => FALSE,
                'type' => 'nested',
                'properties' => [
                  'url' => [],
                  'width' => [
                    'type' => 'integer',
                  ],
                  'height' => [
                    'type' => 'integer',
                  ],
                ]
              ]
            ]
          ]
        ]
      ]
    ];

  }

  private function batchImportList($list, $download) {
    if (!$localFile = $this->getProductList($list, $download)) {
      return;
    }
    $shop = 'affilinet-' . $list['ListID'];

    $streamer = XmlStringStreamer::createStringWalkerParser($localFile);

    while ($node = $streamer->getNode()) {
      $product = simplexml_load_string($node);
      $productInfo = $this->parseProductInfo($product, $shop);
      $this->batchIndex($productInfo, $shop);
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

    if ($download || !file_exists($localFile)) {
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

  private function parseProductInfo($product, $list): array {
    $displayPrice = explode(' ', (string) $product->Price->DisplayPrice);
    switch (count($displayPrice)) {
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

    $categories = explode('&gt;', (string) $product->CategoryPath->ProductCategoryPath);
    $categories = array_map('trim', $categories);

    $indexData = [];
    $indexData['list'] = $list;
    $indexData['price']['value'] = $value;
    $indexData['price']['currency'] = $currency;
    $indexData['articlenumber'] = (string) $product['ArticleNumber'];
    $indexData['link'] = (string) $product->Deeplinks->Product;
    $indexData['title'] = (string) $product->Details->Title;
    $indexData['description'] = (string) $product->Details->DescriptionShort;
    $indexData['keywords'] = $keywords;
    $indexData['categories'] = $categories;
    $indexData['brand'] = (string) $product->Details->Brand;
    $indexData['image']['url'] = (string) $product->Images->Img->URL;
    $indexData['image']['width'] = (string) $product->Images->Img->Width;
    $indexData['image']['height'] = (string) $product->Images->Img->Height;

    return $indexData;
  }

  private function batchIndex($productInfo, $list) {
    if (empty($this->batch['counter'])) {
      $this->batch['counter'] = 1;
    }
    else {
      $this->batch['counter']++;
    }

    $baseParams = $this->getBaseParams();

    $index = [
      'index' => [
        '_index' => $baseParams['index'],
        '_type' => $baseParams['type'],
        '_id' => $list . '-' . $productInfo['articlenumber']
      ]
    ];

    $fields = $productInfo;
    $this->batch['params']['body'][] = $index;
    $this->batch['params']['body'][] = $fields;

    if ($this->batch['counter'] % self::BATCH_SIZE == 0) {
      $this->bulkSend();
    }
  }

  private function bulkSend() {
    try {
      $responses = $this->elasticSearch->bulk($this->batch['params']);
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
    } finally {
      // reset body and cleanup
      $this->batch['params'] = ['body' => []];
      unset($responses);
    }
  }

  private function finishBatch() {
    // Send the last batch if it exists
    if (!empty($this->batch['params']['body'])) {
      $this->bulkSend();
    }
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
    }

    $putParameter = $indexParameter + $aliasParameter;
    $indices->putAlias($putParameter);
    $this->log("Created alias ${aliasParameter['name']} for index ${indexParameter['index']}.", 'info');
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
    if (empty($this->indexName)) {
      $this->getNewIndexName();
    }

    return [
      'index' => $this->indexName,
    ];
  }

  private function getNewIndexName() {
    $counter = 1;
    $prefix = $this->getIndexPrefix();
    $indexName = $prefix . '_' . date('Ymd') . '_';
    $indices = $this->elasticSearch->indices();

    while ($indices->exists(['index' => $indexName . $counter])) {
      $counter++;
    }
    $this->indexName = $indexName . $counter;
  }

  private function getIndexPrefix() {
    return $this->getAliasName();
  }

  private function getAliasParameter() {
    return [
      'name' => $this->getAliasName(),
    ];
  }

  private function getAliasName() {
    return 'nocake';
  }

  /**
   * Remove all indices that have the correct prefix, but do not have an alias
   */
  private function cleanupIndices(){
    $client = $this->elasticSearch;
    $indices = $client->indices();
    $aliases = $indices->getAliases();

    $prefix = $this->getIndexPrefix();

    $filtered = array_filter(
      $aliases,
      function ($value, $index) use ($prefix) {
        $hasPrefix = (strpos($index, $prefix) === 0);
        $hasNoAlias = empty($value['aliases']);

        return ($hasPrefix && $hasNoAlias);
      },
      ARRAY_FILTER_USE_BOTH
    );

    array_walk(
      $filtered,
      function ($value, $index, $indices) {
        $indices->delete(['index' => $index]);
        $this->log("Deleted index $index.", 'info');
      },
      $indices
    );
  }
}
