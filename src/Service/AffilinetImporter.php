<?php

namespace Nocake\Service;

use Elasticsearch\ClientBuilder;
use GuzzleHttp\Client;

class AffilinetImporter implements ImporterInterface
{
    const SOURCES_URL_TEMPLATE = 'http://publisher.affili.net/Download/AutoDownload.aspx?PartnerID=%d&csvPW=%s';
    const DOWNLOAD_URL_TEMPLATE = 'http://productdata.download.affili.net/affilinet_products_%d_%d.xml?auth=%s';
    const PARTNER_ID = '493114';
    const API_PASSWORD = 'qfLfcZ4GPWVJUATGIyCY';

    private $lists = [];
    private $http;
    private $elasticSearch;

    public function __construct()
    {
        $this->http = new Client();
        $this->elasticSearch = ClientBuilder::create()->build();

        libxml_use_internal_errors(true);
    }

    public function import()
    {
        $this->getSources();
        foreach ($this->lists as $list) {
            $this->log ("Importing list for '" . $list['Titel'] . "'", 'info');
            $this->importList($list);
        }
    }

    private function getSources()
    {
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
        if(strpos($contentType, '=') === FALSE) {
            $this->log ("No charset found: $contentType", 'warning');
            $charset = '';
        } else {
            $charset = explode('=', $contentType)[1];
        }

        $catalogSources = (string)$res->getBody();
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

    private function importList($list)
    {
        $listDownloadUrl = sprintf(
          self::DOWNLOAD_URL_TEMPLATE,
          $list['ListID'],
          self::PARTNER_ID,
          self::API_PASSWORD
        );
        $res = $this->http->request('GET', $listDownloadUrl);
        if ($res->getStatusCode() != 200) {
            // something is wrong with this url
            return;
        }

        $products = @simplexml_load_string($res->getBody());
        if(!$products) {
            $this->handleXmlError();
            return;
        }

        unset($res);

        $shop = 'affilinet-'.$products['ProgramID'].'-'.$products['ShopID'];
        $this->deleteShopIndex($shop);

        foreach ($products as $product) {
            $productInfo = $this->parseProductInfo($product, $shop);
            $this->indexProduct($productInfo, $shop);
        }
    }

    private function parseProductInfo($product, $shop)
    {
        $displayPrice = explode(' ', (string)$product->Price->DisplayPrice);
        $keywords = explode(',', (string)$product->Details->Keywords);
        $keywords = array_map('trim', $keywords);

        $indexData = [];
        $indexData['shop'] = $shop;
        $indexData['price'] = $displayPrice[0] + 10;
        $indexData['currency'] = $displayPrice[1];
        $indexData['articlenumber'] = (string)$product['ArticleNumber'];
        $indexData['link'] = (string)$product->Deeplinks->Product;
        $indexData['title'] = (string)$product->Details->Title;
        $indexData['description'] = (string)$product->Details->DescriptionShort;
        $indexData['keywords'] = $keywords;
        $indexData['brand'] = (string)$product->Details->Brand;
        $indexData['image'] = (string)$product->Images->Img->URL;

        return $indexData;
    }

    private function indexProduct($productInfo, $shop)
    {
        $elasticSearchIndexParams = $this->getBaseParams();
        $elasticSearchIndexParams['id'] = $shop . '-' . $productInfo['articlenumber'];
        $elasticSearchIndexParams['body'] = $productInfo;
        $response = $this->elasticSearch->index($elasticSearchIndexParams);
    }

    /**
     * @return array
     */
    private function getBaseParams(): array
    {
        return [
          'index' => 'nocake',
          'type' => 'product',
        ];

    }

    /**
     * @param $shop
     */
    private function deleteShopIndex($shop)
    {
        $shopFilterParams = $this->getBaseParams();

        $shopFilterParams['body'] = [
          'query' => [
            'bool' => [

              'must' => [
                'match' => [ 'shop' => $shop ]
              ],
            ],
          ],
        ];
        try {
            $response = $this->elasticSearch->search($shopFilterParams);
            $this->log ((string) $response['hits']['total'], 'info');
        } catch (\Exception $e) {
            $this->log($e->getMessage(), 'error');
        }
    }

    private function handleXmlError()
    {
        $errors = libxml_get_errors();

        foreach ($errors as $error) {
            $this->log ($error->message, 'warning');
        }

        libxml_clear_errors();
    }

    private function log($message, $type) {
        echo $type . ': ' . trim($message) . PHP_EOL;
    }
}
