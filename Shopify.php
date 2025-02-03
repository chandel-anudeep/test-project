<?php

namespace ShopifyConnector;
use Tygh\Registry;
use Tygh\Languages\Languages;

class Shopify
{
    protected $api_key;
    protected $secret_key;
    public $version = '2025-01';

    public $company_id = 0;
    protected $store_id = 0;
    protected $store_url;
    protected $access_token;

    public $imageUploadType = 'url';
    public $tempSesImgName = 'sess_data';
    public $lang_code = 'en';
    public $languages = [];
    public $warning = false;
    public $message = false;
    protected $dbMapping = ['table'=>'products','field_check'=>'product_code','field_id'=>'product_id'];

    public $connected = false;
    public $error = false;
    public $config = [];
    public $reqHeaders = [];

    public function __construct($company_id = 0) {
        $settings = Registry::get('addons.cm_vendor_signup_advance');
        $this->api_key = trim($settings['api_key']);
        $this->secret_key = trim($settings['secret_key']);

        $this->company_id = is_array($company_id) ? @$company_id['company_id'] : (int)$company_id;

        if( $this->company_id > 0 ){

            $config = is_array($company_id) ? $company_id : db_get_row("SELECT * FROM ?:cm_seller_shopify_details WHERE company_id = ?i",$company_id);
            if( @$config['store_connect'] == 'Y'){

                $this->connected = true;
                $this->store_id = intval($config['store_id']);
                $this->store_url = trim($config['store']);
                $this->access_token = trim($config['access_token']);

                unset($config['store_id'],$config['company_id'],$config['store'],$config['access_token'],$config['store_connect']);
                $config['category_mapping'] = json_decode($config['category_mapping'] ?? '',true) ?? [];
                foreach($config['category_mapping'] as $scid => $ccid){
                    if( empty($ccid) )
                        unset($config['category_mapping'][$scid]);
                }
                $config['product_ids'] = !empty($config['product_ids']) ? explode(',',$config['product_ids']) : [];
                
                $this->config = $config;
            }
        }

        $this->lang_code = CART_LANGUAGE;
        $this->languages = array_keys(Languages::getAll());
    }
    
    //
    // get categories
    public function getCollections() {

        $list = [];

        if ( !$this->connected ){
            $this->error = 'Please first connect Shopify store';
            return [];
        }

        $key = 'sess_collections_'.md5($this->access_token);

        if( !empty($_SESSION[$key]) )
            return $_SESSION[$key];

        $smart_collections = $this->getAllData('smart_collections');
        
        if (!$this->error) {
            if (!empty($smart_collections)) {
                foreach ($smart_collections as $c) {
                    $list[$c['id']] = ucfirst($c['title']);
                }
            }
            $custom_collections = $this->getAllData('custom_collections');

            if (!empty($custom_collections)) {
                foreach ($custom_collections as $c) {
                    $list[$c['id']] = ucfirst($c['title']);
                }
            }
        }
        
        asort($list);
        $_SESSION[$key] =  $list;

        return $list;
    }

    public function getCollectionProducts($next = null, $limit = 250){

        $list = [];
        $total = 0;
        $next_page = null;
        $this->error = false;

        $collections = $this->getCollections();
        if( $this->error )
            return [$list,$total,$next_page];

        $key = 'sess_prdcols_'.md5($this->access_token);
        if( !empty($_SESSION[$key]) )
            $collects = $_SESSION[$key];
        else {
            $_collects = $this->getAllData('collects');
            foreach($_collects as $row){
                $collects[$row['collection_id']][] = $row['product_id'];
            }
            $_SESSION[$key] = $collects;
        }

        if( $this->error )
            return [$list,$total,$next_page];


        $product_ids = [];
        foreach($this->config['category_mapping'] as $col_id => $csId){
            if( isset($collects[$col_id]) ){
                foreach($collects[$col_id] as $_pid){
                    $product_ids[] = $_pid;
                }
            }
        }

        if( empty($product_ids) ){
            $this->error  = "No product found from mapped categoires";
            return [$list,$total,$next_page];
        }

        $product_categories = [];
        foreach($collects as $_col_id => $_pids){
            if( isset($collections[$_col_id]) && isset($this->config['category_mapping'][$_col_id]) ){
                foreach($_pids as $_pid){
                    $product_categories[$_pid] = [
                        'shopify' => $collections[$_col_id],
                        'cscart'=>fn_get_category_name($this->config['category_mapping'][$_col_id])
                    ];
                }
            }
        }
       
        $params = ['ids'=>implode(',',$product_ids)];
        $path = 'products';

        if( !empty($next) ){
            unset($params['collection_id'],$params['ids']);
            $params['page_info'] = $next;
        } else {
            $res_count = $this->call('GET', $path . '/count', $params);

            if (isset($res_count['count']))
                $total= (int)$res_count['count'];
            else {
                if (!$this->error) 
                    $this->error = isset($res_count['errors']) ? $res_count['errors'] : 'Unable to get products from Shopify';
            }
        }

        if( !$this->error && (!empty($next) || $total > 0 ) ){
            $params['limit'] = $limit;
   
            $response = $this->call('GET', $path, $params);

            if (isset($response['errors'])) {
                
                $this->error = is_string($response['errors']) ? $response['errors'] : (array_values($response['errors'])[0]);
                
            }

            if (!empty($response[$path])) {

                foreach ($response[$path] as $p) {
                    $p['categories'] = $product_categories[$p['id']] ?? [];
                    $list[] = $p;
                }

                if (!empty($this->reqHeaders['link'])) {
                    $_link = $this->parseNextLink($this->reqHeaders['link']);
                    if ( !empty($_link) )
                        $next_page  = $_link['page_info'];                      
                }
            }
        }

        return [$list,$total,$next_page];
    }

    // get products
    public function getProducts(){

        $this->error = false;        
        
        $product_ids = $this->config['product_ids'];

        $products = $this->getAllData('products',['ids'=>implode(",",$product_ids)]);
        $collects = [];

        if( !empty($products) ){
            //sleep(1);
            $_collects = $this->getAllData('collects');
            foreach($_collects as $row){
                $collects[$row['product_id']][] = $row['collection_id'];
            }
        }

        $list = [];
        foreach ($products as $row) { 
            if( !empty($row['id']) && !empty($row['title']) && $row['status'] == 'active' && !empty($collects[$row['id']]) ){
                $row['collections'] = $collects[$row['id']];
                $data = $this->parseRow($row,$this->config['category_mapping']);
                if( !empty($data['product_code']) )
                    $list[$row['id']] = $data;
            }
        } 

        return $list;
    }

    // save products 
    public function saveProducts($list, $mode){
        $done = 0;

        foreach($list as $product){
            list($added, $updated, $product_id) = $this->saveProduct($product, $mode);

            //$product_code = $product['product_code'];
            // $this->company_id
            //$old_product_id = 0;


            // save in cscart


            //$product_id = 0;

            //if( $product_id ){
                // shopify_sync_id, shopify_sync_var_id,
               // db_query("UPDATE ?:products SET shopify_sync_product = 'Y' WHERE product_id = ?i",$product_id);

            //}

            $done++;
        }

        return $done;
    }

    public function saveProduct($data, $mode){ 

        $this->warning = false;
        $this->message = false;
        $this->error = false;

        $added = 0;
        $updated = 0;

        $new_product_id = 0;

        $product_code = $data['product_code'];
        $product_id = 0;
        $raw_options = [];
    
        $raw_options =@$data['options'];

        $product_id = $this->checkRecord($product_code, $this->company_id);
        if( empty($product_id) )
                $product_id = (int)db_get_field("SELECT product_id FROM `?:products` WHERE cm_shopify_variant_id = ?i AND company_id = ?i AND `product_code` LIKE ?s ORDER BY product_id ASC", $data['extra']['variant_id'], $this->company_id, $sku);
        if(empty($product_id) && !empty($data['extra']['id']) )
            $product_id = (int)db_get_field("SELECT product_id FROM `?:products` WHERE cm_shopify_product_id = ?i AND company_id = ?i ORDER BY product_id ASC", $data['extra']['id'], $this->company_id);

        $data = $this->parseProductData($product_id, $data);

        $variations = array_values((array)@$data['variations']);
        $options = $raw_options;
        $custom_options = @$data['custom_options'];
        $extra = @$data['extra'];

        if( isset($data['features']) ){
            $features = (array)$data['features'];
            $data['product_features'] = $features = $this->processProductFeatures($features);
        }

        unset($data['variations'],$data['features'],$data['options'],$data['extra'],$data['custom_options']);

        $new_product_id = (int)fn_update_product($data, $product_id);

        if( !$new_product_id ) {
            $this->error = 'Unable to add '. $data['product'];

        } else {

            if( empty($product_id) )
                $added++;
            else
                $updated++;

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
        
        $this->error = !$added && !$updated ? 1 : 0;
        return array($added, $updated, ($product_id ?: $new_product_id));
    }

    protected function checkRecord($val, $check_company = true, $a_cond = null){
        $id = 0;
        if( isset($this->dbMapping) ){
            $cond = '';
            if( $check_company )
                $cond .= ' AND company_id = '.$this->company_id;

            if( !empty($a_cond) )
                $cond .= ' AND '.$a_cond;
            
            $field = $this->dbMapping['field_id'];
 
            $id = (int)db_get_field("SELECT ".$field." FROM ?:".$this->dbMapping['table']." WHERE `".$this->dbMapping['field_check']."` LIKE ?s".$cond." ORDER BY ".$field." ASC LIMIT 0,1",trim($val));
        }
        return $id;
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

    protected function parseProductData($product_id, $data){

        $var_fields = ['product','list_price','price','tracking','weight','length','height','width','full_description','short_description','meta_keywords','search_words','tax_ids','main_image','category_ids'];

        if( !isset($data['category_ids']) && !empty($product_id) ){

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
                    $sp_product_id = (int)db_get_field("SELECT product_id FROM `?:products` WHERE cm_shopify_variant_id = ?i AND company_id = ?i ORDER BY product_id ASC", $var['extra']['variant_id'], $this->company_id);
            }
            
            unset($var['sku'],$var['name'],$var['extra'],$var['combination']);

            $var['product'] = $title;

            foreach($var_fields as $vf){
                if( empty($var[$vf]) && !empty($data[$vf]) )
                    $var[$vf] = @$data[$vf];
            }

            if( !empty($var['length']) || !empty($var['width']) || !empty($var['height']) )
                $var['shipping_params'] = serialize(['min_items_in_box'=>1,'max_items_in_box'=>1,'box_length'=>$var['length'],'box_width'=>$var['width'],'box_height'=>$var['height']]);

            $var['product_id'] = $sp_product_id;
            $var['product_code'] = $sku;
            $var['company_id'] = $this->company_id;  
            $var['combination'] = $combination;
            $v_extra['price'] = @$var['price'];
            //$var['extra'] = $v_extra;
            unset($var['extra']);
            $var['cm_shopify_product_id'] = $v_extra['id'];
            $var['cm_shopify_variant_id'] = $v_extra['variant_id'];
            $var['cm_shopify_sync'] = 'Y';

            $var['price'] = $this->parsePrice(@$var['price']);
            $list_variations[$sku] = $var;        
        }
        
        $variations = $list_variations;
        unset($list_variations);

        $extra['price'] = $price;
        $data['price'] = $this->parsePrice($price);
        if( !empty($data['length']) || !empty($data['width']) || !empty($data['height']) )
            $data['shipping_params'] = serialize(['min_items_in_box'=>1,'max_items_in_box'=>1,'box_length'=>$data['length'],'box_width'=>$data['width'],'box_height'=>$data['height']]);

        $data['company_id'] = $this->company_id;
        //$data['extra'] = $extra;
        $data['cm_shopify_product_id'] = $extra['id'];
        $data['cm_shopify_variant_id'] = $extra['variant_id'];
        $data['cm_shopify_sync'] = 'Y';

        unset($data['extra']);
        $data['variations'] = $variations;

        return $data;
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

    protected function parseRow($data,$collects){
        
        $id = $data['id'];
        $variant = @$data['variants'][0];

        // categories
        $category_ids = [];        
        $pr_collects = (array)@$data['collections'];
     
        foreach($pr_collects as $col_id){
            if( isset($collects[$col_id]) )
                $category_ids[]=(int)$collects[$col_id];
        }

        $category_ids = array_unique($category_ids);
        if( empty($category_ids) )
            return [];

        // images
        $images = [];
        if( !empty($data['images']) ){
            foreach ($data['images'] as $img) {
                $images[$img['id']] = $img['src'];                    
            }
        }

        $main_image = @$images[@$variant['image_id']]; unset($images[@$variant['image_id']]);
        if( empty($main_image) && !empty($images) )
            $main_image = array_values($images)[0];

        $additional_images = array();
        foreach ($images as $img) {
            if( $main_image != $img )
                $additional_images[] = $img;                    
        }
               
        // sku
        $sku = trim(@$variant['sku']);
        if( empty($sku) )
            $sku =  $data['handle'];

        // weight
        $weight = (float)@$variant['weight'];

        list($options,$variations,$sku) = $this->_getVariations($sku,$data);

        $row = [
            'product_code' => $sku,
            'product' => trim($data['title']),
            'full_description' => trim($data['body_html']),
            'category_ids' => $category_ids, 
            'price' => @$variant['price'],
            'list_price' => @$variant['compare_at_price'] ?: '',
            'status' => $data['status'] == 'active' ? 'A' : 'D',
            'tracking' => !empty($variant['inventory_management']) && @$variant['inventory_policy'] == 'deny' ? 'B' : 'D', 
            'amount' => (int)@$variant['inventory_quantity'],
            'weight' => $weight,
            "meta_keywords"=> trim(@$data['tags']),
            "search_words"=> trim(@$data['tags']),
            'main_image' => $main_image,
            'additional_images' => $additional_images,
            'features' => [],
            'options' => $options,
            'variations' => $variations,  
            'extra'=>[
                'id' => $id,
                'variant_id' => @$variant['id']
            ]
        ];
        
        return $row;
    }

    private function _getVariations($sku,$data){

        $options = array();
        $combinations = array();

        $createCombi = !(count($data['variants']) == 1 && strtolower(@$data['variants'][0]['title']) == 'default title');
        if( !$createCombi )
            return array($options,$combinations,$sku);            
        
        if( !empty($data['options']) ){
            foreach( $data['options'] as $var ) {
                $name = trim($var['name']);
                $options[strtolower($name)] = array(
                    'name' => $name,
                    'status' => 'A',
                    'values' => $var['values'],
                );
            }
        }

        $varImages = [];
        if( !empty($data['images']) ){
            foreach( $data['images'] as $_vimg ) {
                if( !empty($_vimg['variant_ids']) ){
                    foreach($_vimg['variant_ids'] as $_vid){
                        if( !isset($varImages[$_vid]) )
                            $varImages[$_vid] = $_vimg['src'];
                    }
                }
            }
        }

        $_options = array_keys($options);
        $parent_sku = null;

        if( !empty($data['variants']) ){
            foreach( $data['variants'] as $var ) {

                $attrs = array();
                foreach($var as $key => $val) {
                    if( strpos($key,'option') !== false && !empty($val) ){
                        $idx = (int)str_replace('option','',$key)-1;
                        if( !empty($_options[$idx]) )
                            $attrs[$_options[$idx]] = $val; 
                    }
                }

                if( !empty($attrs) ){

                    $_sku = !empty($var['sku']) ? trim($var['sku']) : '';
                    if( empty($_sku) )
                        $_sku = $sku."-".preg_replace("/[^a-zA-Z0-9-]+/", "",strtolower(implode("-",array_values($attrs))));

                    if( empty($parent_sku) )
                        $parent_sku = $_sku;

                    //$title = !empty($var['title']) ? trim(stripos($var['title'],$data['title']) !== false ? $var['title'] : $data['title'].' - '.$var['title']) : '';
                    $title = $data['title'];

                    $combinations[] = array(
                        'combination' => $attrs,
                        'sku' => $_sku,
                        'product' => $title,
                        'list_price' => trim(@$var['compare_at_price']),
                        'price'=> @$var['price'],
                        'amount'=> (int)@$var['inventory_quantity'],
                        'tracking' => !empty($var['inventory_management']) && @$var['inventory_policy'] == 'deny' ? 'B' : 'D',
                        'status' => $data['status'] == 'active' ? 'A' : 'D',
                        'main_image' => @$varImages[$var['id']],
                        'weight'=> (float)@$var['weight'],
                        'extra'=>[ 'id' => $var['product_id'], 'variant_id' => $var['id'] ],
                    );   
                }
            }
        }

        if( empty($parent_sku) )
            $parent_sku = $sku;
        
        return [$options,$combinations,$parent_sku];
    }

    //////////////////
    private function getAllData($path, $params = array(), $limit = 250)
    {
        $path = trim($path);
        $list = array();

        $res_count = $this->call('GET', $path . '/count', $params);

        if (isset($res_count['count'])) {
            if ($res_count['count'] > 0) {
                $totalPages = ceil((int)$res_count['count'] / $limit);

                $count = 0;
                $loop = true;

                while ($loop) {
                    $count++;
                    $loop = false;

                    $params['limit'] = $limit;
                    if (!empty($params['page_info']) ) 
                        unset($params['ids'],$params['collection_id'], $params['product_type']);
                    
                    $response = $this->call('GET', $path, $params);

                    if (isset($response['errors'])) {
                        if ($count == 1) 
                            $this->error = $response['errors'];
                        else
                            $this->error = false;
                    }

                    if (!empty($response[$path])) {
                        foreach ($response[$path] as $p) {
                            $list[] = $p;
                        }
                        if (!empty($this->reqHeaders['link'])) {
                            $_link = $this->parseNextLink($this->reqHeaders['link']);
                            if ($_link) {
                                $params['page_info'] = $_link['page_info'];
                                $loop = true;
                            }
                        }
                    }
                }
            }
        } else {
            if (!$this->error) 
                $this->error = isset($res_count['errors']) ? $res_count['errors'] : 'Unable to get data from Shopify';
        }

        return $list;
    }

    public function connect($store_url,$code, $hmac){

        $connected = false;

        $store_url = trim(str_replace(['http://','https://'], '', $store_url),'/');

        $_url = 'https://'.$store_url."/admin/oauth/access_token";
       
        $data = [
            'client_id' => $this->api_key,
            'client_secret' => $this->secret_key,
            'code' => trim($code),
        ];

        $response = $this->request('POST',$_url, [], http_build_query($data));

        if (!empty($response['access_token'])) {
            $_data = [
                'company_id' => $this->company_id,
                "store" => $store_url,
                "access_token" => trim($response['access_token']),
                'store_connect' => 'Y',
            ];
            $connected = $this->saveConfig($_data,true);

        } else 
            $this->error = !empty($response['errors']) ? $response['errors'] : "Unable to authorize, try again";

        return $connected;
    }

    public function saveConfig($data, $ic = false){
        if( empty($data) || !is_array($data) )
            return false;

        $update = true;
        if( $ic )
            $update = !empty(db_get_row("SELECT company_id FROM ?:cm_seller_shopify_details WHERE company_id = ?i",$this->company_id));

        if( $update )
            return db_query("UPDATE ?:cm_seller_shopify_details SET ?u WHERE company_id = ?i",$data,$this->company_id);
        else 
            return db_query("INSERT INTO ?:cm_seller_shopify_details ?e",$data);
    }

    private function parseNextLink($text){
        $link = false;
        if( !empty($text) ){
            $line = false;
            $atext = (array)@explode(',',$text);
            foreach ($atext as $l) {
                if( strpos($l,'rel="next"') !== false )
                    $line = $l;
            }
            if( $line ){

                @preg_match('~<(.*?)>~', $line, $match);
    
                if( !empty($match[1]) ){
                    $_link = trim($match[1]);
                    if( strpos($_link,'https://') !== false ){
                        $uinfo = parse_url($_link);
                        if( !empty($uinfo['query']) ){
                            parse_str ($uinfo['query'],$link);
                            if( empty($link['page_info']))
                                $link = false;
                        } 
                    }
                }    
            }
        }
        return $link;
    }

    public function disconnect() {
        $_url = 'https://'.$this->store_url."/admin/api_permissions/current.json";
       
        $headers = array(
            "Content-Type: application/json",
            "Accept: application/json",
            "X-Shopify-Access-Token: " . $this->access_token
        );

        $response = $this->request('DELETE',$_url, [], [], $headers);

        if(!$response['errors'])
            db_query('DELETE FROM ?:cm_seller_shopify_details WHERE company_id = ?i', $this->company_id);
    }

    public function call($method, $path, $params = [], $data = [], $headers = []) {

        $headers[] = 'Content-Type: application/json';
        $headers[] = 'X-Shopify-Access-Token: ' . $this->access_token;

        $path = trim($path);

        if( strpos($path,'://') === false)
            $url = 'https://' . str_replace(['http://','https://'], '', $this->store_url) . '/admin/api/' . $this->version . '/' . $path . '.json';

        return $this->request($method, $url, $params, $data, $headers);
    }

    public function request($method, $url, $params = [], $data = [], $headers = []) {

        $ch = curl_init();
   
        if (!empty($params)) 
            $url .= '?' . http_build_query($params);
       
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

        if(!empty($headers))
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

        $_headers = [];
        curl_setopt($ch, CURLOPT_HEADERFUNCTION,function($curl, $header) use (&$_headers){
            $len = strlen($header);
            $header = explode(':',$header, 2);
            if (count($header) < 2 || empty($header[0])) // ignore invalid headers
                return $len;

            $_headers[strtolower(trim($header[0]))] = trim($header[1]);
            return $len;
        });

        if ($method != 'GET') {
            if( $method == 'POST')
                curl_setopt($ch, CURLOPT_POST, true);
            else
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);

            if( !empty($data) )
                curl_setopt($ch, CURLOPT_POSTFIELDS, is_string($data) ? $data : json_encode($data));
        }

        $response = json_decode(curl_exec($ch), true);
     
        if (curl_errno($ch)) 
            $this->error = curl_error($ch);
        
        @curl_close($ch);

        if( $this->error )
            $response['errors'] = $this->error;

        $this->reqHeaders = $_headers;
        return $response;
    }

    /////////

    protected function validateHmacAndFetchAccessToken($query) {
        $hmac = $query['hmac'];
        unset($query['hmac']);
        
        $params = [];
        foreach ($query as $key => $val) {
            $params[] = "$key=$val";
        }

        asort($params);
        $params = implode('&', $params);

        $calculated_hmac = hash_hmac('sha256', $params, $this->secret_key);

        if ($hmac === $calculated_hmac) {
           
        } else {
            $this->error = 'Invalid HMAC';
            return false;
        }
    }

    protected function saveAccessTokenToDb($company_id, $_data) {
        db_query('INSERT INTO ?:cm_seller_shopify_details ?e', $_data);
    }

    protected function getAccessTokenFromDb() {
        $access_token = db_get_field('SELECT access_token FROM ?:cm_seller_shopify_details WHERE company_id = ?i', $this->company_id);

        return $access_token ? trim($access_token) : null;
    }

    protected function getStoreFromDb() {
        return db_get_field('SELECT store FROM ?:cm_seller_shopify_details WHERE company_id = ?i', $this->company_id);
    }

}
