<?php
namespace ShopConnector;

use Tygh\Registry;
use Tygh\BlockManager\Block;
use Tygh\Storage;
use Tygh\Languages\Languages;

use Tygh\Addons\ProductVariations\Form\GenerateVariationsForm;
use Tygh\Addons\ProductVariations\Product\Group\GroupFeatureCollection;
use Tygh\Addons\ProductVariations\Request\GenerateProductsAndAttachToGroupRequest;
use Tygh\Addons\ProductVariations\Request\GenerateProductsAndCreateGroupRequest;
use Tygh\Addons\ProductVariations\ServiceProvider;
use Tygh\Addons\ProductVariations\Product\FeaturePurposes;
use Tygh\Addons\ProductVariations\Product\Type\Type;

class Connector
{
    public $company_id = 0;
    public $shop_id = 0;
    public $shop_name = '';
    public $status = 'D';
    
    public $shop_url = '';
    public $type;

    protected $api_url;
    public $error = false;
    public $debug = false;
    
    public $config = [];
    public $credentials = [];
    public $mapping = [];
    public $settings = [];
    public $taxes = [];
    public $categories = [];
    
    protected $warning = false;
    protected $message = false;
    protected $reqHeaders = [];

    protected $allPFTypes = ['csv','xml'];

    protected $dbMapping = [
        'products'=> ['table'=>'products','field_check'=>'product_code','field_id'=>'product_id'],
        'orders'=> ['table'=>'orders','field_check'=>'order_id','field_id'=>'order_id'] 
    ];

    public $mappingTab = false;
    public $hideWeightField = false;
    public $addProductByCronJob = true;

    public $cronJob = false;
    public $webhookCall = false;
    
    public $categoryColumn = false;
    public $extraData = [];

    public $tempDir = DIR_ROOT."/var/shop_connectors/";
   
    public $imageUploadType = 'url';
    public $tempSesImgDir = "/var/custom_files/";
    public $tempSesImgName = 'sess_data';
    public $count_result = [];
    public $detailed_result = [];

    public $lang_code = 'en';
    public $languages = [];
    public $text_feature_ids = [];
    public $webhookType = 'JSON';

    public $enableSync = false;

    public $confirmPopup = false;
    public $confirmMsg = null;
    
	public static function instance($params, $cronjob = false){
        
        $type = $_type = ucfirst(strtolower($params['type'])); 
        $version = @$params['credentials']['version'];
        if( !empty($version) )
            $version = 'V'.str_replace('.','',$version);
       
        $class= str_replace('/','',"\ShopConnector\Type\/$type");
        $_class = $class.$version;
        
        if($version && class_exists($_class) ){        
            $shop = new $_class;
            $shop->setShop($params, $cronjob);
            return $shop;
        } else {
            if( class_exists($class) ){
                $shop = new $class;
                $shop->setShop($params, $cronjob);
                return $shop;
            }
        }

        return false;
    }

    public function updateCredentials($data){

        foreach ($data as $key => $value) {
            $this->credentials[$key] = trim($value);   
        }
        $shop_data = ['credentials' => json_encode($this->credentials)];
        return db_query('UPDATE ?:cm_shop_connectors SET ?u WHERE shop_id = ?i', $shop_data,$this->shop_id);
        
    }
    
    public function setShop($params, $cronjob = false){

        $config = Registry::get('settings.General');

        $this->config = [
            'weight' => floatval(@$config['weight_symbol_grams']),
            'unit' => strtolower(@$config['weight_symbol']),
        ];

        $this->company_id = (int)@$params['company_id'];
        $this->shop_id = (int)@$params['shop_id'];
        $this->shop_name = trim(@$params['name']);
        $this->status = @$params['status'] ?: 'D';

        $this->shop_url = trim(@$params['url']);
        $this->type = ucfirst(strtolower(@$params['type']));
        $this->cronJob = $cronjob;

        $this->credentials = @$params['credentials'];
        $this->settings = @$params['settings'];
        $this->categories = @$params['categories'];
        $this->taxes = @$params['taxes'];
        
        $this->mapping = @$params['mapping']['import'];//$cronjob ? @$params['mapping']['sync'] : ;

        $this->count_result = $cronjob ? @$params['count_sync'] : @$params['count_import'];
        if( !isset($this->count_result['total']) )
            $this->count_result = ['total'=>0,'added'=>0,'updated'=>0,'deleted'=>0,'disabled'=>0];
        
        $this->detailed_result = @json_decode($cronjob ? @$params['detailed_sync'] : @$params['detailed_import'],true) ?: [];

        $this->lang_code = CART_LANGUAGE;
        $this->languages = array_keys(Languages::getAll());
        $this->text_feature_ids = db_get_fields("SELECT feature_id FROM ?:product_features WHERE feature_type = 'T'");

        $this->initShop(@$_REQUEST['action']);
    }

    public function getMapping($key=null){
        return isset($key) ? @$this->mapping[$key] : $this->mapping;
    }
    
    protected function initShop($action){ }
    protected function setApiUrl(){ return ''; }
    protected function setAuthrization(){ return []; }
    
    protected function setContentType(){
        return 'application/json';
    }

    protected function setAcceptType(){
        return 'application/json';
    }
    protected function parseApiUrl($apiUrl,$params =[]){
        return [$apiUrl,$params];
    }

    public function httpCall($method,$path,$parms=[],$data=[],$headers = []){
        
        $noAuth = (bool)@$headers['noauth']; unset($headers['noauth']);
        $noparse = (bool)@$headers['noparse']; unset($headers['noparse']);

        if( strpos($path,'://') !== false )
            $apiUrl = $path;
        else {
            $apiUrl = $this->setApiUrl();
            $apiUrl = rtrim($apiUrl,'/').'/'.trim($path,'/');
        }
        list($apiUrl,$parms) = $this->parseApiUrl($apiUrl,$parms);
        
        if( !empty($parms) )
            $apiUrl .= (strpos($apiUrl,'?') !== false ? '&': '?').http_build_query($parms);
        
        $req_headers = [
          'Content-Type: '.$this->setContentType(),
          'User-Agent: CS-Cart/'.PRODUCT_NAME.'/'.PRODUCT_VERSION.'/Shop-Connector',
        ];

        if( !$noAuth )
            $req_headers = array_merge($req_headers,$this->setAuthrization());
        
        if( !empty($headers) )
            $req_headers = array_merge($req_headers,$headers);

        //if( $method != 'GET' )
         //  $$req_headers[] = 'Accept: '.$this->setAcceptType(); 
        $req_headers = array_unique($req_headers);

        if( $this->error )
            return ['error'=>$this->error];

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $apiUrl);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        
        if( $method != 'GET' ){
          $parms = !empty($data) ? json_encode($data) : '{}';
          //$req_headers[] = 'Content-Length: '.strlen($parms);
          if( $method == 'POST' ) 
            curl_setopt($ch, CURLOPT_POST, 1);
          else 
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST,$method);  
            
          curl_setopt($ch, CURLOPT_POSTFIELDS,$parms);
        }

        curl_setopt($ch, CURLOPT_HTTPHEADER, $req_headers);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        
        $_headers = [];
        curl_setopt($ch, CURLOPT_HEADERFUNCTION, function($curl, $header) use (&$_headers){
            $len = strlen($header);
            $header = explode(':', $header, 2);
            if (count($header) < 2) return $len;
            $_headers[strtolower(trim($header[0]))] = trim($header[1]);
            return $len;
        });
        
        $result = @curl_exec($ch); 
        $hdrs = @curl_getinfo($ch);
        
        $this->reqHeaders = array_merge($hdrs,$_headers);
        if( curl_errno($ch) )
          $this->error = curl_error($ch);
        @curl_close($ch);

        if( $this->debug ){
            echo '<pre>';
            print_r([$apiUrl,$parms,$req_headers,@$hdrs['http_code'],$result]);
            echo '</pre>';
        }

        $result = !$noparse ? $this->parseResult($result) : $result;
        $http_code = @$hdrs['http_code'];
        if( ( empty($result) || !is_array($result) || !empty($result['response']) ) && $http_code > 204 ){

            if( $http_code == 401 )
                $this->error = __("unauthorized_request");

            if( $http_code == 404 )
                $this->error = __("api_url_not_found",['[url]'=>$apiUrl]);
            
            if( !$this->error )
                $this->error = "[$http_code]: ".__("api_no_response");
        }
        
        return $result;
    } 

    protected function parseResult($result){

        $content_type = !empty($this->reqHeaders['content-type']) ? $this->reqHeaders['content-type'] : $this->setContentType();

        if( stripos($content_type,'/xml') !== false){
            
            $xml = new \DOMDocument('1.0','UTF-8');
            $xml->formatOutput = true;
            $res = [];

            $parsed = @$xml->loadXML(str_replace(['<g:','</g:'], ['<g_','</g_'],$result));
            if( $parsed ){
                if( isset($xml->documentElement->tagName) ){
                    $res[$xml->documentElement->tagName] = $this->xmlConvert($xml->documentElement);
                }
            }

            $stype = strtolower($this->type);
            if( isset($res[$stype]))
                $res = $res[$stype];

        } else
            $res = json_decode($result,true);

        if( $res )
            return $res;
        else
            return array('response'=>$result);
    }

    public function xmlConvert($node) {
        $output = array();

        switch ($node->nodeType) {

            case XML_CDATA_SECTION_NODE:
                $output['@cdata'] = trim($node->textContent);
                break;
 
            case XML_TEXT_NODE:
                $output = trim($node->textContent);
                break;
 
            case XML_ELEMENT_NODE:
 
                // for each child node, call the covert function recursively
                for ($i=0, $m=$node->childNodes->length; $i<$m; $i++) {
                    $child = $node->childNodes->item($i);
                    $v = $this->xmlConvert($child);
                    if(isset($child->tagName)) {
                        $t = $child->tagName;
 
                        // assume more nodes of same kind are coming
                        if(!isset($output[$t])) {
                            $output[$t] = array();
                        }
                        $output[$t][] = $v;
                    } else {
                        //check if it is not an empty text node
                        if($v !== '') {
                            $output = $v;
                        }
                    }
                }
 
                if(is_array($output)) {
                    // if only one node of its kind, assign it directly instead if array($value);
                    foreach ($output as $t => $v) {
                        if(is_array($v) && count($v)==1) {
                            $output[$t] = $v[0];
                        }
                    }
                    if(empty($output)) {
                        //for empty nodes
                        $output = '';
                    }
                }
 
                // loop through the attributes and collect them
                if($node->attributes->length) {
                    $a = array();
                    foreach($node->attributes as $attrName => $attrNode) {
                        $a[$attrName] = (string) $attrNode->value;
                    }
                    // if its an leaf node, store the value in @value instead of directly storing it.
                    if(!is_array($output)) {
                        $output = array('@value' => $output);
                    }
                    $output['@attributes'] = $a;
                }
                break;
        }

        if( isset($output['@cdata']) && count($output) == 1 )
            $output = $output['@cdata'];

        return $output;
    }


    //////////////

    protected function getProducts(){ return []; }
    protected function getFields(){ return false; }
    protected function getFeatures(){ return false; }
    protected function getCategories(){ return false; }
    protected function getTaxes(){ return false; }

    ////// 
    protected function getShopData($key,$product_code,$data,$old_id){
        return $data;
    }

    protected function saveSourceData($object_type, $object_id, $id, $extra = [], $reference = null, $uponly = false){

        $object_id = (int)@$object_id;
        if( empty($object_type) || empty($id) || empty($object_id) )
            return false;

        $price = !empty($extra['price']) ? (float)$extra['price'] : 0;
        unset($extra['price']);

        $data = [
            'source_id' => $id,
            'price' => $price,
            'extra' => !empty($extra) ? json_encode($extra) : null,
            'timestamp' => time(),
            'done' => 'Y',
        ];
        if( !empty($reference) )
            $data['reference'] = $reference;

        $cond = db_quote("WHERE object_type = ?s AND shop_id = ?i AND object_id = ?i",$object_type, $this->shop_id, $object_id);

        if( !empty(db_get_array("SELECT source_id FROM ?:cm_shop_connectors_source_data ".$cond)) ){
            db_query("UPDATE ?:cm_shop_connectors_source_data SET ?u ".$cond,$data);

        } else {
            if( !$uponly ){
                $data['object_type'] = $object_type;
                $data['object_id'] = $object_id;
                $data['shop_id'] = $this->shop_id;
                db_query("REPLACE INTO ?:cm_shop_connectors_source_data ?e", $data);
            }
        }
    }

    protected function deleteSourceData($object_type,$object_id){
        db_query("DELETE FROM ?:cm_shop_connectors_source_data WHERE object_type=?s AND shop_id = ?i AND object_id=?i ",$object_type, $this->shop_id, $object_id);
    }

    protected function saveShipment($order_id, $tracking_data, $ref = null){

        //$shipment_id = (int)db_get_field("SELECT object_id FROM ?:cm_shop_connectors_source_data WHERE object_type = ?s AND shop_id =?i AND source_id = ?s",'shipment',$this->shop_id, $ref);

        $old_shipment = db_get_row("SELECT s.shipment_id,s.carrier,s.tracking_number FROM ?:shipments s, ?:shipment_items si WHERE s.shipment_id = si.shipment_id AND si.order_id = ?i GROUP BY si.shipment_id",$order_id);

        $shipment_id = (int)@$old_shipment['shipment_id'];
        unset($old_shipment['shipment_id']);

        $shipment = [
            'carrier' => trim(@$tracking_data['carrier']),
            'tracking_number' => trim(@$tracking_data['tracking_number']),
        ];

        $update = empty($old_shipment) || md5('S'.json_encode($shipment)) != md5('S'.json_encode($old_shipment));

        if( $update ) {

            $_REQUEST['skip_cmsc_shipment_update'] = true;
            $order_info = fn_get_order_info($order_id, false, true, true, false);
            $shipping_id = (int)@$order_info['shipping'][0]['shipping_id'];

            $shipment_products = [];
            foreach((array)@$order_info['products'] as $cart_id => $p){
                $shipment_products[$cart_id] = $p['amount'];
            }
            
            $shipment['order_id'] = $order_id;
            $shipment['shipping_id'] = $shipping_id;
            $shipment['products'] = $shipment_products;
            $shipment['comments'] = '';

            $force_notification = ['C' => true,'A' => true,'V' => true];
        
            $shipment_id = fn_update_shipment($shipment, $shipment_id, 0, false, $force_notification);
            if( $shipment_id && !empty($ref) )
                $this->saveSourceData('shipment', $shipment_id, $ref);

            return !empty($shipment_id);
        }

        return false;
    }

    protected function saveProduct($key,$data){ 

        $this->warning = false;
        $this->message = false;

        $skip = false;

        $added = 0;
        $updated = 0;
        $errored = 0;
        $deleted = 0; 
        $disabled = 0;

        $log = '';
        $new_product_id = 0;

        if( method_exists($this,'additionalData') )
            $data = $this->additionalData($data);

        $product_code = $data['product_code'];
        $product_id = 0;
        $raw_options = [];

        if( $this->checkUnsync($product_code) )
            $skip = true;
        else {

            $product_id = $this->checkRecord('products',$product_code);
            if( empty($product_id) )
                $product_id = (int)db_get_field("SELECT object_id FROM ?:cm_shop_connectors_source_data WHERE object_type IN ('product','variation') AND shop_id = ?i AND reference LIKE ?s ORDER BY object_id ASC",$this->shop_id,$product_code);
            if( empty($product_id) && !empty($data['extra']['id']) )
                $product_id = (int)db_get_field("SELECT object_id FROM ?:cm_shop_connectors_source_data WHERE object_type='product' AND shop_id = ?i AND source_id LIKE ?s ORDER BY object_id ASC",$this->shop_id,$data['extra']['id']);

            if( method_exists($this,'prepareData') )
                $data = $this->prepareData($data, $product_id);
        
            $raw_options =@$data['options'];
            
            $data = $this->parseProductData($product_id, $data, 'P');
            $data = $this->getShopData($key,$product_code,$data,$product_id);
        }

        $variations = array_values((array)@$data['variations']);
        $options = $raw_options;
        $custom_options = @$data['custom_options'];
        $extra = @$data['extra']; 

        if( isset($data['features']) ){
            $features = (array)$data['features'];
            $data['product_features'] = $features = $this->processProductFeatures($features);
        }

        $syncOnly = !$skip && $this->checkSyncOnly($data);

        unset($data['variations'],$data['features'],$data['options'],$data['extra'],$data['custom_options']);

        fn_set_hook('shop_connector_product_save_pre', $product_id, $data, $this->cronJob);

        if( $syncOnly ){
            if( !empty($product_id) ){    
                $new_product_id = $product_id;
                if( !empty($data) )
                    $this->updateProductData($product_id, $data, 0, $extra);
            }
        } else {
        
            if( !$skip ){
                if( $this->cronJob && empty($product_id) )
                    $skip = true;
                else
                    $new_product_id = (int)fn_update_product($data, $product_id);
            }        
        }

        if( !$skip ){

            if( !$new_product_id ){
                if( !$this->cronJob )
                    $log = '<span class="log_error">'.$data['product'].' :  '.__("unable_to_add_update").'</span>';

            } else {

                if( empty($product_id) )
                    $added++;
                else
                    $updated++;

                if( $syncOnly ){

                    $var_ids = [];
                    foreach($variations as $var){
                        
                        $vproduct_id = (int)@$var['product_id'];
                        $vextra = @$var['extra'];
                        unset($var['product_id'],$var['extra'],$var['combination']); 

                        if( !empty($vproduct_id) && !empty($var) && $vproduct_id != $new_product_id ){
                            $var_ids[$vproduct_id] = 1;
                            $this->updateProductData($vproduct_id, $var, $new_product_id, $vextra);
                        }
                    }

                    if( !empty($var_ids) ){
                        $sub_product_ids = db_get_fields("SELECT product_id FROM ?:products WHERE parent_product_id = ?i",$new_product_id);
                        if( !empty($sub_product_ids) )
                            db_query("UPDATE ?:products SET status = 'D' WHERE product_id IN (?n)",$sub_product_ids);

                        foreach($sub_product_ids as $spid){
                            if( empty($var_ids[(int)$spid]) )
                                db_query("UPDATE ?:products SET status = ?s WHERE product_id = ?i", 'D', $vid);
                        }
                    }

                } else {

                    // save shop data
                    $this->saveSourceData('product', $new_product_id, @$extra['id'], $extra, $product_code); 
                    if( !empty($extra['variant_id']) )
                        $this->saveSourceData('variation',$new_product_id, $extra['variant_id'], $extra, $product_code);

                    // save images
                    if( isset($data['main_image']) || isset($data['additional_images']) )
                        $this->saveProductImages($new_product_id,@$data['main_image'],@$data['additional_images']);

                    if( isset($features) || isset($data['product_features']) ){
                        // Delete feature values    
                        db_query("DELETE FROM ?:product_features_values WHERE product_id = ?i",$new_product_id);

                        // save features
                        if( !empty($features) )
                            $this->saveAdditionalFeatures($new_product_id, $features, $this->lang_code);
                    }

                    // save options/variations
                    if( !empty($variations) && !empty($raw_options) ){

                        list($var_total, $var_add, $var_upd) = $this->saveVariations($new_product_id, $variations, $raw_options, $data, $product_code, $custom_options);
                        if( $var_total )
                            $this->message = $var_total." ".strtolower(__("product_variations.variations"))." ($var_add ".__("added").", $var_upd ".__("rss_updated").")";
                    } else {
                        if( isset($custom_options) )
                            $this->saveProductOptions($new_product_id,$custom_options);
                    }
                }

                $log = '<span class="log_'.(!empty($product_id) ? 'warning' : 'success').'">'. (@$product_code ?: $new_product_id) .' :  '.(!empty($product_id) ? strtoupper(__("rss_updated")) : strtoupper(__('new').' '.__("added"))).'!'.($this->message ? ' - <i style="color:#000;"><b>[ '.$this->message.' ]</b></i>' : '').($this->warning ? ' - <i style="color:#f00;">[ '.$this->warning.' ]</i>' : '').'</span>'; 

            }

        } else {
            if( !$this->cronJob )
                $log = '<span class="log_warning">'.$product_code.' : Skipped to import/sync</span>'; 
        }

        if( !empty($log) )
            $this->createLog($log);
        
        $errored = !$added && !$updated ? 1 : 0;
        return array($added, $updated, $errored, $log, ($product_id ?: $new_product_id));
    }

    public function saveAdditionalFeatures($product_id,$features, $lang = CART_LANGUAGE){

        foreach((array)@$features as $feature_id =>$value){
            if( in_array($feature_id,$this->text_feature_ids) ){

                foreach($this->languages as $lang_code){

                    if( $lang_code != $lang){

                        db_query('DELETE FROM ?:product_features_values WHERE feature_id = ?i AND product_id = ?i AND lang_code = ?s', $feature_id, $product_id, $lang_code);

                        db_query('INSERT INTO ?:product_features_values ?e', [
                            'product_id' => $product_id,
                            'feature_id'=>$feature_id,
                            'lang_code' => $lang_code,
                            'value' => $value,
                        ]);
                    }
                }

            }
        }
    }

    public function updateProductData($product_id, $data, $parent_id, $extra = []){

        $company_id = @$data['company_id'];
        $product_code = @$data['product_code'];

        if( !$parent_id && isset($data['category_ids']) ){
            $rebuild = false;
            fn_update_product_categories($product_id, $data['category_ids'], $rebuild);
        }

        if( isset($data['price']) )
            fn_update_product_prices($product_id, $data);

        if( !empty($data['main_image']) || !empty($data['additional_images']) )
              $this->saveProductImages($product_id, @$data['main_image'], @$data['additional_images']);
                        
        unset($data['company_id'],$data['product_code'],$data['category_ids'],$data['price'],$data['main_image'],$data['additional_images']);

        $desc_data = [];
        $desc_fields = ['product','full_description','short_description','search_words','meta_keywords'];
        foreach($desc_fields as $fld){
            if( isset($data[$fld]) ){
                $desc_data[$fld] = $data[$fld];
                unset($data[$fld]);
            }
        }

        if( !empty($data['tax_ids']) )
            $data['tax_ids'] = implode(",",$data['tax_ids']);
        
        if( !empty($data) )
            db_query("UPDATE ?:products SET ?u WHERE product_id = ?i", $data, $product_id);

        if( !empty($desc_data) )
            $this->updateProductDesc($product_id, $desc_data);

        if( !empty($extra) ){
            $this->saveSourceData($parent_id ? 'variation' : 'product', $product_id, @$extra[$parent_id ? 'variant_id' : 'id'], $extra, $product_code); 
            if( !$parent_id ) 
                $this->saveSourceData('variation', $product_id, @$extra['variant_id'], $extra, $product_code); 
        }        
    }

    public function saveVariations($product_id, $variations, $options, $data, $product_code, $custom_options){

        list($feature_ids, $selected_options, $purpose) = $this->saveFeatures($options,$this->lang_code);

        $total = $added = $updated = 0;

        $sub_products = [];
        $done_sp_ids = [];  

        $sub_product_ids = db_get_fields("SELECT product_id FROM ?:products WHERE parent_product_id = ?i",$product_id);

        if( !empty($sub_product_ids) )
            db_query("UPDATE ?:products SET status = 'D' WHERE product_id IN (?n)",$sub_product_ids);

        foreach ($variations as $pos => $var) {

            if( !empty($var['combination']) && !empty($var['product_code']) ){
                
                $combination = $var['combination'];
                $sp_product_id = (int)@$var['product_id'];
                if( !empty($sp_product_id) )
                    unset($var['product_code']);

                unset($var['combination'],$var['product_id']);

                $combi_code = [];                        
                foreach ($combination as $ckey => $cvalue){
                    $ckey = strtolower(trim($ckey));
                    $cvalue = strtolower(trim($cvalue));
                    if( !empty($selected_options[$ckey]['values'][$cvalue]) ){
                        $fid = $selected_options[$ckey]['feature_id'];
                        $vid = $selected_options[$ckey]['values'][$cvalue];
                        
                        if( !isset($features_ids[$fid]) ) $features_ids[$fid] = 1;
                        $combi_code[$fid] = $vid;
                    }
                }

                if( !empty($combi_code) ){

                    $_code = [];
                    foreach ($combi_code as $fid=>$vid) {
                        $_code[] = $fid.'_'.$vid;
                    }
                    $_code = implode('_',$_code);

                    //$var['product_features'] = (array)@$var['product_features'];
                    $var['product_features'] = $features = !empty($var['features']) ? $this->processProductFeatures($var['features']) : [];

                    if( isset($var['features']) )
                       $var['features'] = $features;

                    foreach($combi_code as $fid =>$vid){
                        $var['product_features'][$fid] = $vid;                               
                    }

                    $sub_products[$_code] = ['product_id'=>$sp_product_id, 'data'=>$var];
                }   
            }
        }

        if( !empty($sub_products) ){

            // get group id
            $group_code = 'PV-'.$product_id;
            $group_id = (int)@db_get_field("SELECT id FROM ?:product_variation_groups WHERE code LIKE ?s",$group_code);
            if( !$group_id )
                $group_id = (int)db_query("INSERT INTO ?:product_variation_groups ?e",['code'=>$group_code,'created_at'=>time(),'updated_at'=>time()]);

            if( $group_id ){

                $first = false;
                
                // get first product
                $_sb_pd_fvs = db_get_array("SELECT fv.product_id,fv.feature_id, fv.variant_id FROM ?:products p, ?:product_features_values fv, ?:product_variation_group_features vgf WHERE fv.product_id = p.product_id AND (p.product_id = ?i OR p.parent_product_id = ?i) AND vgf.purpose = ?s AND vgf.feature_id = fv.feature_id AND vgf.group_id = ?i GROUP BY fv.product_id,fv.feature_id",$product_id, $product_id, $purpose, $group_id);

                $__sb_pd_fvs = [];
                foreach($_sb_pd_fvs as $r) {
                    $__sb_pd_fvs[$r['product_id']][] = $r['feature_id'].'_'.$r['variant_id'];
                }
                $sb_pd_fvs = [];
                foreach ($__sb_pd_fvs as $pid => $keys) {
                    $sb_pd_fvs[implode('_',$keys)] = $pid;
                    rsort($keys);
                    $sb_pd_fvs[implode('_',$keys)] = $pid;      
                }
                foreach ($sub_products as $idx => $spdata){
                    $_code = [];
                    foreach((array)@$spdata['data']['product_features'] as $fid => $vid){
                        $_code[] = $fid.'_'.$vid;
                    }
                    if( !empty($_code) ){
                        $_code = implode('_', $_code);
                        if( empty($sub_products[$idx]['product_id']) && !empty($sb_pd_fvs[$_code]) ){
                            $spid = $sb_pd_fvs[$_code];
                            if( !empty($spid) && empty($spdata['product_id']) )
                               $sub_products[$idx]['product_id'] = $spid;
                            if( $spid == $product_id ){
                                $first = $spdata['data'];
                                unset($sub_products[$idx]);
                            }
                        }
                    }
                }
                
                if( !$first ){
                    foreach ($sub_products as $idx => $sp) {
                        if( empty($sp['product_id']) && @$sp['data']['product_code'] == $product_code ){
                            $first = $sp['data'];
                            unset($sub_products[$idx]);
                        }
                        if( !empty($sp['product_id']) && $sp['product_id'] == $product_id ){
                            $first = $sp['data'];
                            unset($sub_products[$idx]);
                        }
                    }
                }
                
                $sub_products = array_values($sub_products);
                if( !$first ){
                    $first = $sub_products[0]['data']; 
                    unset($sub_products[0]); 
                }

                $total = count($sub_products);

                $haveMore = $total > 0;
            
                // delete var data
                db_query("DELETE FROM ?:product_variation_group_features WHERE group_id = ?i",$group_id);
                db_query("DELETE FROM ?:product_variation_group_products WHERE group_id = ?i",$group_id);
                db_query("DELETE FROM ?:product_variation_group_products WHERE (product_id = ?i OR parent_product_id = ?i)",$product_id,$product_id);

                foreach ($feature_ids as $fid) {
                    db_query("INSERT INTO ?:product_variation_group_features ?e",[
                        'feature_id'=>$fid,
                        'purpose'=> $purpose,
                        'group_id'=>$group_id,
                    ]);
                }
                
                // update first variaton                 
                $this->saveSourceData('variation', $product_id, @$first['extra']['variant_id'], @$first['extra'], @$first['product_code']);
                    
                // save var features
                fn_update_product_features_value($product_id, (array)@$first['product_features'],[], $this->lang_code);
                    
                // save price
                if( isset($first['price']) )
                    fn_update_product_prices($product_id, $first);

                // save desc
                if( !empty($first['product']) )
                    $this->updateProductDesc($product_id,['product'=>$first['product']]);

                // save options
                if( isset($custom_options) )
                    $this->saveProductOptions($product_id,$custom_options);

                // save features
                if( !empty($first['features']) )
                    $this->saveAdditionalFeatures($product_id,$first['features'],$this->lang_code);

                unset($first['extra'],$first['product_features'],$first['category_ids'],$first['price']);

                $first['parent_product_id'] = 0;
                $first['product_type'] = 'P';
                
                db_query("UPDATE ?:products SET ?u WHERE product_id = ?i", $first, $product_id); 
                
                db_query("REPLACE INTO ?:product_variation_group_products ?e",[
                    'product_id'=>$product_id,
                    'parent_product_id'=>0,
                    'group_id'=> $group_id,
                ]);

                if( $haveMore ){                
                    // saving variants
                    
                    $savedSkus = [];

                    foreach($sub_products as $pdata){

                        $_data = $pdata['data'];
                        $sku = @$_data['product_code'];

                        if( !empty(db_get_field("SELECT product_id FROM ?:products WHERE product_id =?i",$product_id)) && empty($savedSkus[$sku]) ){
                                
                            if( !empty($sku) )
                                $savedSkus[$sku] = 1;

                            $extra = @$_data['extra'];
                            unset($_data['extra']);

                            $old_sp_id = (int)@$pdata['product_id'];
                            
                            $sp_product_id = (int)fn_update_product($_data,$old_sp_id);

                            if( !empty($sp_product_id) ){

                                if( $old_sp_id )
                                    $updated++;
                                else
                                    $added++;

                                // save desc
                                if( isset($_data['product']) )
                                    $this->updateProductDesc($sp_product_id,['product'=>$_data['product']]);
    
                                $sp_data = [
                                    'parent_product_id' => $product_id,
                                    'product_type' => 'V',
                                ];
                                
                                if( isset($_data['status']) )
                                    $sp_data['status'] = $_data['status'];
                                
                                if( isset($_data['shipping_params']) )
                                    $sp_data['shipping_params'] = $_data['shipping_params'];

                                db_query("UPDATE ?:products SET ?u WHERE product_id = ?i",$sp_data,$sp_product_id);
                                    
                                // source data
                                $this->saveSourceData('variation', $sp_product_id, @$extra['variant_id'], $extra, @$_data['product_code']);

                                // save images
                                if( isset($_data['main_image']) || isset($_data['additional_images']) )
                                    $this->saveProductImages($sp_product_id,@$_data['main_image'],@$_data['additional_images']);
                                
                                // insert var group
                                $r = db_query("REPLACE INTO ?:product_variation_group_products ?e",[
                                    'product_id'=>$sp_product_id,
                                    'parent_product_id'=>$product_id,
                                    'group_id'=> $group_id,
                                ]);

                                // save options
                                if( isset($custom_options) )
                                    $this->saveProductOptions($sp_product_id,$custom_options);

                                // save features
                                if( isset($_data['features']) )
                                    $this->saveAdditionalFeatures($sp_product_id,$_data['features'],$this->lang_code   );

                                if( $r )
                                    $done_sp_ids[] = $sp_product_id;
                            
                            }
                        }
                    }
                }
            }
        }

        return [$total, $added, $updated];
    }

    public function updateProductDesc($product_id,$data){
        if( empty($product_id) || empty($data) )
            return false;
        foreach ($this->languages as $langcde) {
            db_query("UPDATE ?:product_descriptions SET ?u WHERE product_id = ?i AND lang_code = ?s", $data, $product_id, $langcde);       
        }
        return true;
    }

    protected function saveFeatures($features,$lang_code){

        $purpose = 'group_catalog_item';

        $selected_options = $feature_ids = [];
        
        foreach($features as $fid => $fdata){

            $name = ucfirst(trim($fdata['name']));
            $code = strtolower(trim(preg_replace('/-+/', '-',preg_replace('/[^A-Za-z0-9\-]/','',str_replace(' ', '-',$name)))));

            if( isset($fdata['id']) )
                $feature = db_get_row("SELECT f.feature_id,f.feature_type,f.parent_id,f.status FROM ?:product_features f WHERE f.feature_id = ?i", $fdata['id']);
            else
                $feature = db_get_row("
                    SELECT f.feature_id,f.feature_type,f.parent_id,f.status
                    FROM ?:product_features f, ?:product_features_descriptions fd 
                    WHERE f.feature_id = fd.feature_id AND f.purpose = ?s AND trim(f.categories_path) = ''
                        AND fd.lang_code = ?s AND (fd.description LIKE ?s OR f.feature_code = ?s OR f.feature_code = ?s)
                ", $purpose, $lang_code, $name, $name, $code);

            if( empty($feature['feature_id']) ){
                $_data =[
                    'internal_name' => $name,
                    'description' => $name,
                    'company_id' => '0',
                    'purpose' => $purpose,
                    'feature_style' => 'dropdown',
                    'filter_style' => 'checkbox',
                    'feature_type' => 'S',
                    'parent_id' => '0',
                    'feature_code' => $code,
                    'position' => '',
                    'full_description' => '',
                    'status' => @$fdata['status'] ?: 'A',
                    'display_on_product' => 'Y',
                    'display_on_catalog' => 'Y',
                    'display_on_header' => 'N',
                    'prefix' => '',
                    'suffix' => '',
                    'categories_path' => '' 
                ];

                $variants = [];
                if( !empty($fdata['values']) ){
                    foreach($fdata['values'] as $val) {
                        $variants[] =[
                            'position' =>  '',
                            'color' => '#ffffff',
                            'variant' => ucfirst(trim($val)),
                            'description' => '',
                            'page_title' => '',
                            'url' => '',
                            'meta_description' => '',
                            'meta_keywords' => '',
                        ];
                    }
                }

                $_data['variants'] = $variants;

                $feature_id = fn_update_product_feature($_data,0, $lang_code);
                if( $feature_id ){
                    $feature = [
                        'feature_id'=>$feature_id,
                        'feature_type'=>$_data['feature_type'],
                        'parent_id'=>$_data['parent_id'],
                        'status'=>$_data['status'],
                    ];
                }
            }

            if( !empty($feature['feature_id']) && !empty($fdata['values']) ){

                if( (int)$feature['parent_id'] > 0 || $feature['status'] != 'A' )
                    db_query("UPDATE ?:product_features SET ?u WHERE feature_id = ?i",['parent_id'=>0,'status'=>'A'],$feature['feature_id']);

                $feature_id = (int)$feature['feature_id'];
                $feature_ids[] = $feature_id;

                $selected_options[strtolower($name)] = ['feature_id'=>$feature_id];

                foreach ($fdata['values'] as $value){

                    $value = trim($value);
                    
                    $variant_id = (int)@db_get_field("Select vd.variant_id from ?:product_feature_variants v,?:product_feature_variant_descriptions vd WHERE v.variant_id = vd.variant_id AND vd.variant LIKE ?s AND v.feature_id = ?i AND vd.lang_code = ?s",$value,$feature_id,$lang_code);
                    
                    if( !$variant_id ){
                        $variant_id = (int)db_query("INSERT INTO ?:product_feature_variants (`feature_id`,`url`,`position`) VALUES ('$feature_id','','0')");
                        if( $variant_id ){
                            foreach ($this->languages as $langcde) {
                                db_query("INSERT INTO ?:product_feature_variant_descriptions (`variant_id`,`variant`,`lang_code`,`description`,`page_title`,`meta_keywords`,`meta_description`) VALUES (?i,?s,?s,'','','','')",$variant_id,$value,$langcde);       
                            }
                        }
                    }
                    if( $variant_id )
                        $selected_options[strtolower($name)]['values'][strtolower($value)] = $variant_id;
                }
            }
        }

        return [$feature_ids, $selected_options, $purpose];
    }

    public function checkUnsync($product_code){
        $skip = false;

        if( empty($product_code) )
            $skip = true;
        else {

            $skus = trim(@$this->settings['product_skus_unsync']);
            if( !empty($skus) ){
                $skus = explode(",",str_replace("\n",",",$skus));
                foreach($skus as $_i => $sku){
                    $skus[$_i] = strtoupper(trim($sku)); 
                    if( empty($skus[$_i]) )
                        unset($skus[$_i]);
                }
            }

            if( !empty($skus) && in_array(strtoupper($product_code),$skus) )
                $skip = true;
        }

        return $skip;
    }

    public function checkSyncOnly($data){
        $check = false;

        if( $this->cronJob && $this->enableSync ){
            $check = true;
            if( !empty($data['category_ids']) && !empty($data['main_image']) && !empty($data['product']) && !empty($data['full_description']) )
                $check = false;
        }
        return $check;
    }

    public function createLog($log){
        
        $log = trim(rtrim(strip_tags($log),'!'));
        $type = $this->webhookCall ? 'WEBHOOK' : ($this->cronJob ? 'CRONJOB' : 'IMPORT');
        
        $dir = $this->tempDir.'logs';
        if( !is_dir($dir) )
            mkdir($dir,0777,true);

        $log = date("Y-m-d H:i:s")." [$type] ".$log.PHP_EOL;

        @file_put_contents($dir.'/'.$this->shop_id.'_'.date('Ymd').'.log', $log, FILE_APPEND);
    }

    /////

    protected function checkRecord($type, $val, $check_company = true, $a_cond = null){
        $id = 0;
        if( isset($this->dbMapping[$type]) ){
            $cond = '';
            if( $check_company )
                $cond .= ' AND company_id = '.$this->company_id;

            if( !empty($a_cond) )
                $cond .= ' AND '.$a_cond;
            
            $field = $this->dbMapping[$type]['field_id'];
 
            $id = (int)db_get_field("SELECT ".$field." FROM ?:".$this->dbMapping[$type]['table']." WHERE `".$this->dbMapping[$type]['field_check']."` LIKE ?s".$cond." ORDER BY ".$field." ASC LIMIT 0,1",trim($val));
        }
        return $id;
    }
 
    protected function parsePrice($price){

        if( !empty($price) && !empty($this->settings['price']['rate']) ){

            $mapPrice = $this->settings['price'];
            $rate = floatval(trim(str_replace('%','',$mapPrice['rate'])));
            $price = floatval($price);
            
            if( $price > 0 ){    
                $extraPrice = 0;

                if( $rate > 0 ){ 
                    if( @$mapPrice['type'] == 'A')
                        $extraPrice = $rate;
                    else 
                        $extraPrice = number_format($price*$rate/100,2,'.','');
                }

                $price = @$mapPrice['action'] =='D' ? $price-$extraPrice : $price+$extraPrice;
                if( $price < 0) $price = 0;
            }
        }
        return $price;
    }

    protected function getRequiredFields($product_id, $data, $type = 'P'){

        if( $type == 'V' )
            unset($data['variations']);

        $fields_modify = [];
        if( !empty($product_id) ){

            if( $this->enableSync && $this->cronJob && !empty($this->settings['fields_sync']) )
                $fields_modify = $this->settings['fields_sync'];
            else if( !empty($this->settings['fields_override']) )
                $fields_modify = $this->settings['fields_override'];

            foreach($fields_modify as $key => $check){
                if( !(int)$check ){
                    $key = explode(',',$key);
                    foreach($key as $_key){
                        if( trim($_key) !== '')
                            unset($data[$_key]);
                    }
                }
            }

        }

        return $data;
    }  

    protected function parseProductData($product_id, $data, $type = "P"){

        $var_fields = ['product','list_price','price','tracking','weight','length','height','width','full_description','short_description','meta_keywords','search_words','tax_ids','main_image','category_ids'];

        if( !isset($data['category_ids']) && !empty($product_id) && @$type == "P" ){

            $category_ids = array_map('intval',db_get_fields("SELECT category_id FROM ?:products_categories WHERE product_id = ?i ORDER BY link_type DESC,category_position ASC",$product_id));

            foreach($data['category_ids'] as $cid){
                if( !in_array((int)$cid, $category_ids) )
                    $category_ids[] = $cid;
            }
            $data['category_ids'] = array_unique($category_ids);
        }

        $product_code = $data['product_code'];
        $price = floatval(!empty($data['price']) ? $data['price'] : 0);
        $variations = (array)@$data['variations'];
        $extra = @$data['extra']; 

        unset($data['extra'],$data['variations']);

        if( !empty($product_id) )
            unset($data['product_code']);

        if( !in_array(strtolower($this->type), $this->allPFTypes))
            unset($data['product_features']);

        if( $type == 'P' ){
            $list_variations = [];
            foreach($variations as $idx => $var){

                $sku = @$var['sku']; 
                $title = @$var['name'] ?: @$var['product'];
                $v_extra = $var['extra'];
                $combination = $var['combination'];

                $sp_product_id = 0; 
                if( $product_id > 0 ){
                    $sp_product_id = (int)db_get_field("SELECT product_id FROM ?:products WHERE product_code LIKE ?s AND parent_product_id = ?i ORDER BY product_id ASC", $sku, $product_id);
                    if( empty($sp_product_id) )
                        $sp_product_id = (int)db_get_field("SELECT object_id FROM `?:cm_shop_connectors_source_data` WHERE `object_type` = 'variation' AND shop_id = ?i AND `reference` LIKE ?s ORDER BY object_id ASC", $this->shop_id, $sku);
                    if(empty($sp_product_id) && !empty($var['extra']['variant_id']) )
                        $sp_product_id = (int)db_get_field("SELECT object_id FROM ?:cm_shop_connectors_source_data WHERE object_type='variation' AND shop_id = ?i AND source_id LIKE ?s ORDER BY object_id ASC",$this->shop_id,$var['extra']['variant_id']);
                }
                
                unset($var['sku'],$var['name'],$var['extra'],$var['combination']);

                $var['product'] = $title;

                foreach($var_fields as $vf){
                    if( empty($var[$vf]) && !empty($data[$vf]) )
                        $var[$vf] = @$data[$vf];
                }

                if( !empty($var['length']) || !empty($var['width']) || !empty($var['height']) )
                    $var['shipping_params'] = serialize(['min_items_in_box'=>1,'max_items_in_box'=>1,'box_length'=>$var['length'],'box_width'=>$var['width'],'box_height'=>$var['height']]);

                $var = $this->getRequiredFields($sp_product_id, $var, 'V');
              
                if( !empty($var) ){
                    $var['product_id'] = $sp_product_id;
                    $var['product_code'] = $sku;
                    $var['company_id'] = $this->company_id;  
                    $var['combination'] = $combination;
                    $v_extra['price'] = @$var['price'];
                    $var['extra'] = $v_extra;
                    $var['price'] = $this->parsePrice(@$var['price']);
                } 
                $list_variations[$sku] = $var;        
            }
            
            $variations = $list_variations;
            unset($list_variations);
        }

        $extra['price'] = $price;
        $data['price'] = $this->parsePrice($price);
        if( !empty($data['length']) || !empty($data['width']) || !empty($data['height']) )
            $data['shipping_params'] = serialize(['min_items_in_box'=>1,'max_items_in_box'=>1,'box_length'=>$data['length'],'box_width'=>$data['width'],'box_height'=>$data['height']]);

        $data = $this->getRequiredFields($product_id, $data, $type);

        if( !empty($data) ){
            $data['company_id'] = $this->company_id;
           // $data['product_type'] = $type;
            $data['extra'] = $extra;
        }
        $data['variations'] = $variations;

        return $data;
    }

    protected function getWeight($value, $unit=null){

        $value = floatval($value);

        if( empty($value) )
            return $value;

        if( empty($unit) )
            $unit = @$this->settings['weight_unit'] ?: 'lbs';
        $unit = strtolower(trim($unit));
            
        $c_unit = !empty($this->config['unit']) ? $this->config['unit'] : "lbs";
        $c_value = (float)@$this->config['weight'] ?: 453.592; 

        if( $unit == 'g' || $unit == 'grms' || $unit == 'gram' || $unit == 'grams') $unit = 'grm';
        if( $unit == 'kgs' || $unit == 'kilogram' || $unit == 'kilograms' || $unit == 'kilo') $unit = 'kg';
        if( $unit == 'pound' || $unit == 'pounds' || $unit == 'lb' ) $unit = 'lbs';
        if( $unit == 'ounce' || $unit == 'ounces' ) $unit = 'oz';

        if( $unit == $c_unit )
            return $value;

        $value_g = $value;
        if( $unit != 'grm' ){
            if( $unit == 'kg'  ) $value_g = ceil($value * 1000);
            if( $unit == 'lbs' ) $value_g = ceil($value * 453.592);
            if( $unit == 'oz'  ) $value_g = ceil($value * 28.3495);
        }

        return round($value_g / $c_value,3);
    }

    protected function getMapIdsCSCart($type,$ids,$prefix=false){

        $mutli = is_array($ids) ? true : false;
        if( !$mutli )
            $ids = [$ids];

        $mapIds = (array)@$this->$type;
        if( empty($mapIds) || empty($ids) )
            return [];

        $ids = array_unique($ids);
        $_ids = array();
        foreach ($ids as $id) {
            if( $prefix )
                $id = $prefix.":".$id;

            foreach ($mapIds as $key => $value) {
                if( strtolower($key)==strtolower($id) && !empty($key) && !empty($id) && !empty($value) ){
                    if( $type == 'categories' && !empty($this->mapping) ){
                        if( (int)@$this->mapping[$key] || $this->webhookCall || $this->cronJob )
                            $_ids[] = $value;    
                    } else 
                        $_ids[] = $value;
                }
            }
        }

        return $mutli ? array_unique($_ids) : @$_ids[0];
    }

    protected function getMapIdsShop($type,$ids,$prefix=false){

        $mutli = is_array($ids) ? true : false;

        if( !$mutli )
            $ids = [$ids];

        $mapIds = (array)@$this->$type;

        if( empty($mapIds) || empty($ids) )
            return array();

        $_ids = array();
        foreach ($ids as $value) {
           
            foreach($mapIds as $m_id => $c_id) {
                if( !empty($c_id) && trim($c_id) == trim($value) && !empty($value) ){
                    if( $prefix ){
                        if( strpos($m_id,$prefix) === false)
                            continue;
                        $m_id = str_replace($prefix.':','',$m_id);   
                    }

                    $_ids[] = $m_id;
                }
            }   
        }

        return $mutli ? $_ids : @$_ids[0];
    }
    
    
    protected function saveProductOptions($product_id,$options,$lang_code = CART_LANGUAGE){

        $product_options = array();
        db_query("UPDATE ?:product_options SET status='D' WHERE product_id = ?i",$product_id);
        foreach ($options as $code => $option) {
            
            if( !empty($option['name']) && !empty($option['values']) ){

                $name = trim($option['name']);
                $option_id = (int)db_get_field("Select o.option_id from ?:product_options o, ?:product_options_descriptions od WHERE o.product_id = ?i AND o.option_id = od.option_id AND od.lang_code = ?s AND od.option_name LIKE ?s limit 0,1",$product_id, $lang_code, $name);
                
                $reqData = array(
                    'product_id' => $product_id,
                    'option_name' => $name,
                    'company_id' => 0,
                    'option_type' => 'S',
                    'required' => 'Y',
                    'variants' => array(),
                    'status' => 'A',
                );
                
               // if (fn_allowed_for('MULTIVENDOR')) $reqData['company_id'] = $this->company_id;

                if( !empty($option['values']) ){

                    foreach($option['values'] as $p => $value ){
                            
                        $variant_id = (int)db_get_field("Select v.variant_id from ?:product_option_variants v, ?:product_option_variants_descriptions vd WHERE v.option_id = ?i AND v.variant_id = vd.variant_id AND vd.lang_code = ?s AND vd.variant_name LIKE ?s",$option_id,  $lang_code, $value['label']);
                        
                        $reqData['variants'][$p] = array( 
                            'position' => $p,
                            'variant_name' => $value['label'],
                            'modifier' => $value['price'],
                            'modifier_type' => 'A',
                            'weight_modifier' => 0,
                            'weight_modifier_type' => 'A',
                            'status' => 'A',
                            'point_modifier' => '',
                            'point_modifier_type' => 'A',
                            'variant_id' => $variant_id,
                        );  
                    }
                }

                $option_id = fn_update_product_option($reqData,$option_id,$lang_code);

                $product_options[] = $option_id;
                /*
                $_ovalueList = db_get_array("Select v.variant_id,vd.variant_name from ?:product_option_variants v, ?:product_option_variants_descriptions vd WHERE v.option_id = ?i AND v.variant_id = vd.variant_id AND vd.lang_code = ?s",$option_id,  $lang_code);

                $optionValues = array();
                foreach($_ovalueList as $_ovd){
                    $optionValues[strtolower(trim($_ovd['variant_name']))] = (int)$_ovd['variant_id'];  
                }

                if( $option_id )
                    $product_options[strtolower($name)] = array('id'=>$option_id, 'values' => $optionValues);
                */
            }
        }

        $del_opt_ids = db_get_fields("SELECT option_id FROM ?:product_options WHERE status='D' AND product_id = ?i",$product_id);
        if( !empty($del_opt_ids) ){
            foreach($del_opt_ids as $d_op_id){
                fn_delete_product_option($d_op_id, $product_id);
            }
        }

        return $product_options;
    }
    
    protected function processProductFeatures($features,$lang_code = CART_LANGUAGE){
        $product_features = array();
       
        foreach( $features as $key => $_feature ){

            $name = trim($_feature['name']);
            if( empty($name) )
                continue;

            $type = trim(@$_feature['type'] ?: 'T');

            $purpose = 'describe_product';
            $feature_style = 'text';

            if( $type == 'S' || $type == 'M' )
                $purpose = 'find_products';
            
            if( $type == 'M')
                $feature_style = 'multiple_checkbox';

            $code = strtolower(trim(preg_replace('/-+/', '-',preg_replace('/[^A-Za-z0-9\-]/','',str_replace(' ', '-',$name)))));

            $feature = db_get_row("SELECT f.feature_id,f.feature_style, f.purpose,f.feature_type, f.parent_id,f.status FROM ?:product_features f, ?:product_features_descriptions fd WHERE f.feature_id = fd.feature_id AND fd.lang_code = ?s AND (fd.description LIKE ?s OR f.feature_code = ?s)",$lang_code,$name,$code);

            if( empty($feature['feature_id']) ){
                $_data =[
                    'internal_name' => $name,
                    'description' => $name,
                    'company_id' => '0',
                    'purpose' => $purpose,
                    'feature_style' => $feature_style,
                    'filter_style' =>  $type == 'I' ? '' : 'checkbox',
                    'feature_type' => $type,
                    'parent_id' => '0',
                    'feature_code' => $code,
                    'position' => '',
                    'full_description' => '',
                    'status' => 'A',
                    'display_on_product' => 'Y',
                    'display_on_catalog' => 'Y',
                    'display_on_header' => 'N',
                    'prefix' => '',
                    'suffix' => '',
                    'categories_path' => '' 
                ];

                $feature_id = fn_update_product_feature($_data,0);
                if( $feature_id ){
                    $feature = [
                        'feature_id'=>$feature_id,
                        'feature_type'=>$_data['feature_type'],
                        'feature_style'=>$_data['feature_style'],
                        'purpose'=>$_data['purpose'],
                        'status'=>$_data['status'],
                        'parent_id'=>$_data['parent_id'],
                    ];
                }
            }

            if( !empty($feature['feature_id']) ){

                $feature_id = (int)$feature['feature_id'];
                if( (int)$feature['parent_id'] > 0)
                    db_query("UPDATE ?:product_features SET ?u WHERE feature_id = ?i",['parent_id'=>0]);

                $type = $feature['feature_type'];
                $feature_style = $feature['feature_style'];
                $value = $feature_style != 'multiple_checkbox' ? (is_array($_feature['value']) ? $_feature['value'] : trim($_feature['value'])) : @$_feature['value'];

                if( $type != 'T' )
                    $value = !empty($value) ? (is_array($value) ? $value : [$value]) : [];

                if( $type == 'T')
                    $product_features[$feature_id] = $value;
                else {
                  
                    $variant_ids =[];
                    foreach($value as $val){

                        $variant_id = (int)@db_get_field("Select vd.variant_id from ?:product_feature_variants v,?:product_feature_variant_descriptions vd WHERE v.variant_id = vd.variant_id AND vd.variant LIKE ?s AND v.feature_id = ?i AND vd.lang_code = ?s",$val,$feature_id,$lang_code);

                        if( !empty($variant_id) )
                            $variant_ids[] = $variant_id;

                        else{

                            $variant_id = (int)db_query("INSERT INTO ?:product_feature_variants (`feature_id`,`url`,`position`) VALUES ('$feature_id','','0')");

                            if( $variant_id ){
                                foreach ($this->languages as $langcde) {
                                    db_query("INSERT INTO ?:product_feature_variant_descriptions (`variant_id`,`variant`,`lang_code`,`description`,`page_title`,`meta_keywords`,`meta_description`) VALUES (?i,?s,?s,'','','','')",$variant_id,$val,$langcde);
                                }     
                                $variant_ids[] = $variant_id;
                            }
                        }
                    }
                    $product_features[$feature_id] = $type == 'M' ? $variant_ids : @$variant_ids[0];
                }
            }
        }

        return $product_features;
    }

    protected function saveProductImages($product_id,$main_image,$additional_images){

        $uType = $this->imageUploadType;
        $imageData = [];
        if( !empty($main_image) ){
            $imageData = array(
                'product_main_image_data' => array(array(
                    'pair_id' => '',
                    'type' => 'M',
                    'object_id' => 0,
                    'image_alt' => '', 
                    'detailed_alt' => '',
                )),
                'file_product_main_image_detailed' => array($main_image),
                'type_product_main_image_detailed' => array($uType),
            );

            $imageData['product_add_additional_image_data'] = array();
            $imageData['file_product_add_additional_image_detailed'] = array();
            $imageData['type_product_add_additional_image_detailed'] = array();
        }

        if( !empty($additional_images) ){
            foreach ($additional_images as $key => $image) {
                $imageData['product_add_additional_image_data'][$key] =array(
                    'pair_id' => '',
                    'type' => 'A',
                    'object_id' => 0,
                    'image_alt' => '', 
                    'detailed_alt' => '',
                );
                $imageData['file_product_add_additional_image_detailed'][$key] =$image;
                $imageData['type_product_add_additional_image_detailed'][$key] = $uType;
            }
        } 
        
        if( $product_id && !empty($main_image) && !empty($additional_images) ) 
            fn_delete_image_pairs($product_id, 'product');
        
        if( !empty($main_image) ){
            $r = $this->attachImagePairs($imageData, 'product_main', 'product', $product_id);
            if( empty($r) )
                $this->warning = __("main_image_not_saved");
        }
       
        if( !empty($additional_images) ){
            $this->attachImagePairs($imageData, 'product_additional', 'product', $product_id);
            $this->attachImagePairs($imageData, 'product_add_additional', 'product', $product_id);
        }
        
        $this->cleanExtraImages($product_id,$main_image,$additional_images);
        
        return $imageData;
    }

    private function attachImagePairs($data, $name, $object_type, $object_id = 0, $lang_code = CART_LANGUAGE, $object_ids = array())
    {
        if( class_exists('\Tygh\Tools\ImageHelper') )
            $allowed_extensions = \Tygh\Tools\ImageHelper::getSupportedFormats($object_type);
        else
            $allowed_extensions = ['png', 'gif', 'jpg', 'jpeg', 'ico','webp'];
    
        $allowed_file_size_bytes = function_exists('fn_get_allowed_image_file_size') ?  fn_get_allowed_image_file_size() : false;
    
        $icons = $this->filterUploadedImageData($data, $name . '_image_icon', $allowed_extensions, $allowed_file_size_bytes);
        $detailed = $this->filterUploadedImageData($data, $name . '_image_detailed', $allowed_extensions, $allowed_file_size_bytes);
        
        $pairs_data = !empty($data[$name . '_image_data']) ? $data[$name . '_image_data'] : array();
        
        return fn_update_image_pairs($icons, $detailed, $pairs_data, $object_id, $object_type, $object_ids, true, $lang_code);
    }

    private function filterUploadedImageData($data, $name, $filter_by_ext = array(), $filter_by_size = [])
    {
        $udata_local = fn_rebuild_files('file_' . $name);
        $udata_other = !empty($data['file_' . $name]) ? $data['file_' . $name] : array();
        $utype = !empty($data['type_' . $name]) ? $data['type_' . $name] : array();

        if (empty($utype)) {
            return array();
        }

        $filtered = array();

        foreach ($utype as $id => $type) {
            if ($type == 'local' && !fn_is_empty(@$udata_local[$id])) {
                $filtered[$id] = fn_get_local_data(Bootstrap::stripSlashes($udata_local[$id]));

            } elseif ($type == 'server' && !fn_is_empty(@$udata_other[$id]) && (Registry::get('runtime.skip_area_checking') || AREA == 'A')) {
                fn_get_last_key($udata_other[$id], 'fn_get_server_data', true);
                $filtered[$id] = $udata_other[$id];

            } elseif ($type == 'url' && !fn_is_empty(@$udata_other[$id])) {
                fn_get_last_key($udata_other[$id], 'fn_get_url_data', true);
                $filtered[$id] = $udata_other[$id];
            } elseif ($type == 'uploaded' && !fn_is_empty(@$udata_other[$id])) {
                fn_get_last_key($udata_other[$id], function ($file_path) {
                    return fn_get_server_data($file_path, array(Storage::instance('custom_files')->getAbsolutePath('')));
                }, true);

                $filtered[$id] = $udata_other[$id];
            }

            if (isset($filtered[$id]) && $filtered[$id] === false) {
                unset($filtered[$id]);
                continue;
            }

            if (!empty($filtered[$id]['name'])) {
                $filtered[$id]['name'] = \Tygh\Tools\SecurityHelper::sanitizeFileName(urldecode($filtered[$id]['name']));
                
                if (!fn_check_uploaded_data($filtered[$id], $filter_by_ext, $filter_by_size)) {
                    unset($filtered[$id]);
                }
            }
        }

        static $shutdown_inited;

        if (!$shutdown_inited) {
            $shutdown_inited = true;
            register_shutdown_function('fn_remove_temp_data');
        }

        return $filtered;
    }

    private function cleanExtraImages($product_id,$main_image,$additional_images){
        $list = [];
        if( !empty($main_image) )
            $list[] = $main_image;
        if( !empty($additional_images) )
            $list = !empty($list) ? array_merge($list,$additional_images) : $additional_images;

        $server_url = Registry::get('config.current_location');
        foreach ($list as $img) {

            if( strpos($img,$server_url) !== false )
                $img = str_replace($server_url.'/',DIR_ROOT.'/',$img);    
            else if( strpos("#".$img,"#".$this->tempSesImgName.'/') !== false )
                $img = DIR_ROOT.$this->tempSesImgDir.$img;

            if( file_exists($img) ){
                @unlink($img);
            }
        }
    }


    /////

    public function refresh($type){

        $object_types = [];
        if( $type == 'products')
            $object_types = ['product','variation'];

        if( !empty($object_types) )
            db_query("UPDATE ?:cm_shop_connectors_source_data SET done = 'N' WHERE object_type IN (?a) AND shop_id = ?i", $object_types, $this->shop_id);
    }

    public function finshed($type){

        $object_types = [];
        $ids = [];

        $deleted = $disabled = 0;

        if( $type == 'products')
            $object_types = ['product','variation'];

        if( !empty($object_types) )
            $ids = db_get_array("SELECT object_id, object_type FROM ?:cm_shop_connectors_source_data WHERE object_type IN (?a) AND shop_id = ?i AND done = 'N'", $object_types, $this->shop_id);

        if( $type == 'products' && !empty($ids) )
            return $this->cleanExtreaProducts($type,$ids);

        return [$deleted,$disabled];
    }

    public function cleanExtreaProducts($type,$ids){

        $unsync_action = @$this->settings['unsync_action'];
        $object_types = [];
        $deleted = $disabled = 0;
        
        if( $type == 'products')
            $object_types = ['product','variation'];

        // remove/delete
        if( $unsync_action == 'R' ){
            foreach($ids as $row){
                if( $type == 'products'){
                    if( fn_delete_product($row['object_id']) )
                        $deleted++;
                }
            }
        }

        // disable
        if( $unsync_action == 'D' ){
            foreach($ids as $row){
                if( $type == 'products' ){
                    if( db_query("UPDATE ?:products SET status = 'D' WHERE product_id = ?i ", $row['object_id']) )
                        $disabled++;
                }
            }

            db_query("UPDATE ?:cm_shop_connectors_source_data SET done='Y' WHERE object_type IN (?a) AND shop_id = ?i AND done = 'N'", $object_types, $this->shop_id);
        }        

        return [$deleted,$disabled];
    }

    protected function loadImage($_images,$single=false){

        $dir = strtolower($this->type);
        $images = [];

        $_images = $single ? [$_images] : $_images;

        if( $this->imageUploadType == 'url' ){
            foreach ($_images as $img) {
                $images[] = $img;
            }
                    
            return $single ? trim(@$images[0]) : $images;
        }

        if( $this->imageUploadType == 'local' )
            $imgDir = DIR_ROOT.'/images/'.$dir;
        else
            $imgDir = DIR_ROOT.$this->tempSesImgDir.$this->tempSesImgName;
            
        if( !is_dir($imgDir) )
            mkdir($imgDir,0777,true);

        $server_url = Registry::get('config.current_location');

        $imgUrl = $server_url.'/images/'.$dir.'/';

        $locaTypes = ['local','uploaded'];

        foreach ($_images as $img) {
            
            $ext = explode('?',pathinfo($img,PATHINFO_EXTENSION))[0];
            if( in_array(strtolower($ext),['jpeg','png','gif','jpg','webp']) && strpos($img,'?') !== false )
                $img = explode('?',$img)[0];
            else
                $ext = "jpg";

            $name = $this->shop_id."_".md5($img).'.'.$ext;

            $imgPath = $imgDir.'/'.$name;
            $_url = $imgUrl.$name;

            if( file_exists($imgPath) ){
                if ( $this->imageUploadType == 'local' )
                    $images[] = $imgPath;
                else
                    $images[] = $this->tempSesImgName.'/'.$name;

            } else {

                $ch = curl_init();
                curl_setopt($ch, CURLOPT_URL, $img );
                curl_setopt($ch, CURLOPT_HEADER, 0);
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
                curl_setopt($ch, CURLOPT_USERAGENT, "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0");
                //curl_setopt($ch, CURLOPT_HEADER, TRUE);
                curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
                curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 15); 
                curl_setopt($ch, CURLOPT_TIMEOUT, 15); //timeout in seconds
       
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);

                $imgData = @curl_exec($ch); 
                $cinfo = @curl_getinfo($ch);
                if( @$cinfo['http_code'] <= 204 ){
                    if( !empty($imgData) ){
                        @file_put_contents($imgPath,$imgData);
                        if( file_exists($imgPath) ){
                            if ( $this->imageUploadType == 'local' )
                                $images[] = $imgPath;
                            else
                                $images[] = $this->tempSesImgName.'/'.$name;
                        }
                    }
                }
            }            
        }

        return $single ? trim(@$images[0]) : $images;
    }

    public function getData($type,$data=[]){
        
        $data = false;

        if( $type == 'products'){
            $data = [];
            $category_mapping = (array)$this->mapping;
            foreach( $category_mapping as $category => $enabled ){
                if( !(int)$enabled ) 
                    unset($category_mapping[$category]);
            }
            
            if( empty($category_mapping) )
                $this->error = "Please map atleast one category to import products";
            else {

                $_data = $this->getProducts();
                if( !empty($_data))
                    $this->error = false; 
              
                foreach($_data as $key => $value) {
                    $data[] = $value;           
                }
            }
        }

        if( in_array($type, array('categories','taxes','fields','features') ) ){
            $mthd  = 'get'.ucfirst($type);
            if( method_exists($this, $mthd) ){
                $list = $this->$mthd();
                $data = array('list'=>$list,'error'=>$this->error);
            } else 
                return false;
        }

        return $data;
    }

    public function saveData($type,$key,$data){
        $result = array(false,false,'');
        if( $type == 'products' )
            $result = $this->saveProduct($key,$data);   
        return $result;   
    }

    public function parsePhone($phone){
        $phone = substr(trim(preg_replace('/[^0-9\+]+/', '', $phone)),"-10");
        return $phone;
    }

    /// WEBHOOK

    public function manageWebhooks(){

        $error = false;
        $webhook_url = fn_url('shop_connectors.webhook?shop_id='.$this->shop_id,'C');

        if( method_exists($this,'saveWebhooks')){
            $error = $this->saveWebhooks($webhook_url);
        }

        return $error;
    }

    public function handleWebhook($_data, $_headers = []){
        
        $this->webhookCall = true;
        $this->cronJob = false;

        $processed = false;
        if( !empty($_data) && method_exists($this,'processWebhook')){
                
            if( $this->webhookType == 'JWT')
                $_data = base64_decode(trim(@explode('.',$_data)[1]));
            
            $_data = @json_decode($_data,true);
            if( !empty($_data) ){
                
                $resp = $this->processWebhook($_data,$_headers);

                if( is_array($resp) && count($resp) === 4 ){
                    list($type, $id, $data, $action) = $resp;
                
                    if( is_string($type) && !empty($id) && (!empty($data) || $action =='delete') ){

                        if( $type == 'product'){
                            
                            if( $action == 'update' || $action == 'add' ){
                                list($added, $updated, $errored, $log, $pid) = $this->saveProduct(0,$data);
                                if( !empty($pid) && !empty($log) && ($updated || $added))
                                    $processed = true;
                            }

                            if( $action == 'delete' ){
                                $ids = db_get_array("SELECT object_id,object_type FROM ?:cm_shop_connectors_source_data WHERE object_type IN ('product','variation') AND shop_id = ?i AND source_id LIKE ?s",$this->shop_id, $id);

                                if( !empty($ids) ){
                                    list($deleted,$disabled) = $this->cleanExtreaProducts('products',$ids);
                                    if( $deleted || $disabled )
                                        $processed = true;
                                }
                            }
                        }
                    }
                }    
                   
                if( !empty($resp) && is_bool($resp) )
                    $processed = true;
            }        
        }
        
        return $processed;
    }

    public function getCached($key, $time = 60, callable $fn) // time in minutes
    {   
        $key = 'cmsc_cache_'.strtolower($this->type).'_'.md5(strtolower($key).'_'.json_encode($this->credentials));
        
        $check = (array)@$_SESSION[$key];
        $data = @$check['data'];

        $call = empty($check['time']);
        if( !empty($check['time']) ){
            if( (time() - $check['time']) > $time*60 )
                $call = true;
        }

        if( $call ){
            $data = $fn();
            if( !empty($data) ){
                $check['data'] = $data;
                $check['time'] = time();
                $_SESSION[$key] = $check;
            }
        }

        return $data;
    }
}
