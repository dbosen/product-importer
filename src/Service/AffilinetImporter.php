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

  private $lists = [];
  private $http;
  private $elasticSearch;

  public function __construct() {
    $this->http = new Client();
    $this->elasticSearch = ClientBuilder::create()->build();

    libxml_use_internal_errors(TRUE);
  }

  public function import() {
    $this->getSources();
    foreach ($this->lists as $list) {
      $this->log("Importing list for '" . $list['Titel'] . "'", 'info');
      $this->importList($list);
    }
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
    $this->lists = $csv->data;
  }

  private function log($message, $type) {
    echo $type . ': ' . trim($message) . PHP_EOL;
  }

  private function importList($list) {
    if(!$localFile = $this->downloadProductList($list)) {
      return;
    }

    $shop = 'affilinet-' .  $list['ListID'];
    $this->deleteShopIndex($shop);

    $streamer = XmlStringStreamer::createStringWalkerParser($localFile);

    while ($node = $streamer->getNode()) {
      $product = simplexml_load_string($node);
      $productInfo = $this->parseProductInfo($product, $shop);
      $this->indexProduct($productInfo, $shop);
    }

  }

  private function handleXmlError() {
    $errors = libxml_get_errors();

    foreach ($errors as $error) {
      $this->log($error->message, 'warning');
    }

    libxml_clear_errors();
  }

  /**
   * @param $list
   */
  private function deleteShopIndex($list) {
    // TBD
    return;


    $shopFilterParams = $this->getBaseParams();

    $shopFilterParams['body'] = [
      'query' => [
        'bool' => [
          'filter' => [
            'term' => ['description' => 'Herren Krawatte von HUGO, Reine Seide, Schmale Form, Leicht schimmernde Textur, Made in Italy, Breite: 6 cm']
          ],
          'must' => [
            'match_all' => (object) []
          ],
        ],
      ],
    ];
    try {
      $response = $this->elasticSearch->search($shopFilterParams);
      $this->log((string) $response['hits']['total'], 'info');
      print_r($response);
    } catch (\Exception $e) {
      $this->log($e->getMessage(), 'error');
    }
  }

  /**
   * @return array
   */
  private function getBaseParams(): array {
    return [
      'index' => 'nocake',
      'type' => 'product',
    ];

  }

  private function parseProductInfo($product, $list) {
    $displayPrice = explode(' ', (string) $product->Price->DisplayPrice);
    $keywords = explode(',', (string) $product->Details->Keywords);
    $keywords = array_map('trim', $keywords);

    $indexData = [];
    $indexData['list'] = $list;
    $indexData['price'] = $displayPrice[0] + 10;
    $indexData['currency'] = $displayPrice[1];
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
    $response = $this->elasticSearch->index($elasticSearchIndexParams);
  }

  /**
   * Download a remote product list.
   *
   * @param $list
   *
   * @return bool|string $localFile
   */
  private function downloadProductList($list) {
    $listDownloadUrl = sprintf(
      self::DOWNLOAD_URL_TEMPLATE,
      $list['ListID'],
      self::PARTNER_ID,
      self::API_PASSWORD
    );

    $urlPath = parse_url($listDownloadUrl)['path'];
    $localTmpPath = sys_get_temp_dir() . '/' . self::DOWNLOAD_PATH;
    if (!file_exists($localTmpPath)) {
      if (!mkdir($localTmpPath, 0777, TRUE)) {
        $this->log('Could not create download directory ' . $localTmpPath, 'error');
        exit();
      }
    }

    $localFile = $localTmpPath . '/' . basename($urlPath);

    $res = $this->http->request('GET', $listDownloadUrl, ['sink' => $localFile]);
    if ($res->getStatusCode() != 200) {
      // something is wrong with this url
      return FALSE;
    }
    return $localFile;
  }
}
