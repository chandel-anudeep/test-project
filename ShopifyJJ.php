<?php

namespace ShopConnector\Type;

use ShopConnector\Connector;

class Shopify extends Connector 
{
    public $version= '2024-01';

    // configure
    public $mappingTab = true;
    public $hideWeightField = true;
    // public $enableSync = true;

    protected $webhooks = ['products/update','products/delete','orders/updated','inventory_items/update'];

    public function configure(){
        return [
            'label' => 'Shopify',
            'url_label' => "Store Url (eg. storename.myshopify.com)",
            'fields' => [
                'access_type' => [
                    'label' => 'Access Type',
                    'type'=>'select', 
                    'options'=>[
                        'token'=>'Access Token', 
                        'keys'=>'Api Key / Api Secret'
                    ], 'required' => true
                ],
                'access_token' => ['label' => 'Access Token', 'type'=>'text', 'required' => true,'depend_on'=>"access_type:token"],
                'api_key' => ['label' => 'Api Key', 'type'=>'text','required' => true,'depend_on'=>"access_type:keys"],
                'secret_key' => ['label' => 'Api Secret', 'type'=>'password', 'required' => true,'depend_on'=>"access_type:keys"],
            ],
            'allow_sync' => true,
        ];
    }

    protected function setApiUrl(){

        $url = str_replace(['http://','https://'],'',$this->shop_url).'/admin/api/'.$this->version;

       // if( @$this->credentials['access_type'] == 'keys' )
       //     $url = trim(@$this->credentials['api_key']).':'.trim(@$this->credentials['secret_key']).'@'.$url;

        return 'https://'.$url;
    }

    protected function parseApiUrl($apiUrl,$params =[]){
        return [$apiUrl.'.json',$params];
    }

    protected function setAuthrization(){

        if( @$this->credentials['access_type'] == 'keys' )
            return ['Authorization: Basic '.base64_encode(trim(@$this->credentials['api_key']).':'.trim(@$this->credentials['secret_key']))];

        return [  
            'X-Shopify-Access-Token: '.trim(@$this->credentials['access_token']),
        ];
    }

    // un-sync shipment
    public function unsyncShipments($shipments){
        $error = false;

        foreach($shipments as $shipment_id => $data){
            $this->httpCall('POST','fulfillments/'.$data['source_id'].'/cancel');
        }

        return $error;
    }

    // sync shipment
    public function syncShipments($shipment, $products, $oinfo, $shipment_data){

        if( empty($shipment_data['tracking_number']) )
            return "Tracking number is required";

        $tracking_info = [
            'company' => ucfirst(@$shipment_data['carrier']),
            'number' => strtoupper(@$shipment_data['carrier']).": ".$shipment_data['tracking_number'],
        ];

        $request = [
            'notify_customer' => false,
            'tracking_info' => $tracking_info,
        ];

        $shopify_order_id  = $oinfo['source_id'];
        $shopify_fulfil_id = @$shipment['source_id'];
        
        $order_id    = $oinfo['object_id'];
        $shipment_id = $shipment_data['shipment_id'];
        $error       = false;

        if( !empty($shopify_fulfil_id) ){

            $res = $this->httpCall('POST', 'fulfillments/'.$shopify_fulfil_id.'/update_tracking', [], ['fulfillment'=>$request]);
            $error = @$res['errors'] ?: @$res['error'];
            if( !empty($error) && is_array($error) )
                $error = array_values($error)[0];

            if( empty($error) )
                $error = "Unable to create fulfillment in Shopify";

            return $error;
        }

        $_shp_order = $this->httpCall('GET', 'orders/'. $shopify_order_id .'/fulfillment_orders');
        $shp_order = null;
        if( !empty($_shp_order['fulfillment_orders']) )
        foreach($_shp_order['fulfillment_orders'] as $_sfo){
            if( @$_sfo['status'] != 'closed'){
                $shp_order = $_sfo;
                break;
            }
        }

        $fulfillment_order_id = @$shp_order['id'];
        $location_id = @$shp_order['assigned_location_id'];

        if( empty($fulfillment_order_id) || empty($location_id) )
            return "No shopify fullfill order fund with id: ".$shopify_order_id.", please verify/enable all order and fullfill related permissions in Shopify app";
        
        $line_items = [];
        foreach($products as $product){
            $var_id = @$product['extra']['variant_id'];
            if( !empty($var_id) )
                $line_items[$var_id] = $product['qty'];
        }

        $order_items = [];
        foreach((array)@$shp_order['line_items'] as $line){
            $var_id = $line['variant_id'];
            if( isset($line_items[$var_id]) ){   
                $order_items[] = [
                    'id'=>$line['id'],
                    'quantity'=> $line_items[$var_id],
                ];
            }
        }

        if( empty($shopify_fulfil_id) ){
            $request["location_id"] = $location_id;
            $request["line_items_by_fulfillment_order"] = [
                [                              
                    'fulfillment_order_id' => $fulfillment_order_id,
                    'fulfillment_order_line_items' => array_values($order_items),
                ]
            ];
        }

        $res = $this->httpCall('POST', 'fulfillments', [], ['fulfillment' => $request]);

        if( !empty($res['fulfillment']['id']) ){
            $this->saveSourceData('shipment', $shipment_id, $res['fulfillment']['id'], [
                'ff_order_id' => $fulfillment_order_id,
            ], str_replace('.','-F',@$res['fulfillment']['name']) );

        } else {
            $error = @$res['errors'] ?: @$res['error'];
            if( !empty($error) && is_array($error) )
                $error = array_values($error)[0];

            if( empty($error) )
                $error = "Unable to create fulfillment in Shopify";
        } 

        return $error;
    }

    protected function handleOrderShipments($order_id,$shopify_order_id,$order_ref,$products,$order,$order_info){

        if( empty($shopify_order_id) || empty($order_id) || empty($order_info['shipment_ids']) )
            return false;

        $oinfo = [
            'source_id' => $shopify_order_id,
            'price' => $order['total_price'],
            'reference' => $order_ref,
            'object_id' => $order_id,
        ]; 

        $shipment_ids = $order_info['shipment_ids'];

        foreach($shipment_ids as $shipment_id){
            $shipment = db_get_row("SELECT source_id, price, reference FROM ?:cm_shop_connectors_source_data WHERE object_type = ?s AND shop_id=?i AND object_id = ?i", 'shipment', $this->shop_id, $shipment_id);
            $shipment_data = db_get_row("SELECT * FROM ?:shipments WHERE shipment_id = ?i",$shipment_id);

            $this->syncShipments($shipment, $products, $oinfo, $shipment_data);
            break;
        }

        return true;
    }

    // sync order
    public function syncOrder($info, $products, $order_info, $deleted){

        $log = '';
        $error = false;

        $shopify_order_id = @$info['source_id'];
        $order_id         = (int)$order_info['order_id'];
        $status           = $order_info['status'];

        $paid             = in_array($status, fn_get_order_paid_statuses()) && $status != "O";
        $isCancelled      = in_array($status, ['I','F','D','B']);
        $order_ref        = !empty($info['reference']) ? $info['reference'] : "#".$order_id."-".rand(999,101);
        
        if( empty($shopify_order_id) && !$paid )
            return 'Skipped - Not paid ';

        if( empty($shopify_order_id) && ( $deleted || $isCancelled ) )
            return 'Skipped due to status: '.$order_info['status'];

        $domain = str_ireplace(['demo.','dev.','staging.'],'',trim($_SERVER['SERVER_NAME']));
        $phone  = $this->parsePhone($order_info['phone']);

        $financial_status   = $paid ? 'paid' : 'pending';
        $fulfillment_status = null;

        $subtotal = array_sum(array_column(array_values($products),'total'));
        $discount = (float)$order_info['subtotal_discount'];
        $tax      = (float)$order_info['tax_subtotal']; 
        $shipping_cost = (float)$order_info['shipping_cost'];

        //$total = $subtotal+$tax+$shipping_cost - $discount;
        //if( $total < 0) $total = 0;
        $total = (float)$order_info['total'];
        
        $customer = [
            'first_name' => !empty($order_info['firstname']) ? trim($order_info['firstname']) : '',
            'last_name' => !empty($order_info['lastname']) ? trim($order_info['lastname']) : '',
            'email' => $order_info['email'],
        ];
        
        $billing_address = [
            'first_name' => !empty($order_info['b_firstname']) ? trim($order_info['b_firstname']) : $customer['first_name'],
            'last_name' => !empty($order_info['b_firstname']) ? trim($order_info['b_lastname']) : $customer['last_name'],
            'phone' => !empty($order_info['b_phone']) ? $this->parsePhone($order_info['b_phone']) : $phone,
            'address1' => !empty($order_info['b_address']) ? trim($order_info['b_address']) : '',
            'address2' => !empty($order_info['b_address_2']) ? trim($order_info['b_address_2']) : '',
            'city' => !empty($order_info['b_city']) ? trim($order_info['b_city']) : '',
            'province' => !empty($order_info['b_state_descr']) ? trim($order_info['b_state_descr']) : '',
            'zip' => !empty($order_info['b_zipcode']) ? trim($order_info['b_zipcode']) : '',
            'country' => !empty($order_info['b_country_descr']) ? trim($order_info['b_country_descr']) : '',
            'countryCode' => !empty($order_info['b_country']) ? trim($order_info['b_country']) : '',
        ];

        $shipping_address = [
            'first_name' => !empty($order_info['s_firstname']) ? trim($order_info['s_firstname']) : $customer['first_name'],
            'last_name' => !empty($order_info['s_lastname']) ? trim($order_info['s_lastname']) : $customer['last_name'],
            'phone' => !empty($order_info['s_phone']) ? $this->parsePhone($order_info['s_phone']) : $phone,
            'address1' => !empty($order_info['s_address']) ? trim($order_info['s_address']) : '',
            'address2' => !empty($order_info['s_address_2']) ? trim($order_info['s_address_2']) : '',
            'city' => !empty($order_info['s_city']) ? trim($order_info['s_city']) : '',
            'province' => !empty($order_info['s_state_descr']) ? trim($order_info['s_state_descr']) : '',
            'zip' => !empty($order_info['s_zipcode']) ? trim($order_info['s_zipcode']) : '',
            'country' => !empty($order_info['s_country_descr']) ? trim($order_info['s_country_descr']) : '',
            'countryCode' => !empty($order_info['s_country']) ? trim($order_info['s_country']) : '',
        ];

        $line_items = [];
        foreach($products as $product){
            $var_id = @$product['extra']['variant_id'];
            $prd_id = @$product['extra']['id'];

            if( empty($var_id) && $product['type'] == 'V')
                $var_id = @$product['source_id'];
            if( empty($prd_id) && $product['type'] == 'P')
                $prd_id = @$product['source_id'];

            $item = ['quantity' => $product['qty'],'price'=>$product['price']];
            if( !empty($var_id) )
                $item['variant_id'] = $var_id;
            if( empty($var_id) && !empty($prd_id) )
                $item['product_id'] = $var_id;

            if( empty($var_id) && empty($prd_id) )
                $item['title'] = $product['name'];
        
            $line_items[] = $item;
        }

        $shipping_lines = [];
        if( !empty($order_info['shipping']) ){
            $shp = $order_info['shipping'][0];
            $shipping_lines[] = [
                'title' => $shp['shipping'],
                'price' =>  $shp['rate'],
            ];
        }

        $order = [
            'reference' => "#$order_id ($domain)",
            'contact_email' => $order_info['email'],
            'email' => $order_info['email'],
            'currency' => CART_PRIMARY_CURRENCY,
            'financial_status' => $financial_status,
            'fulfillment_status' => $fulfillment_status,
            'note' => trim($order_info['notes']),
        //    'phone' => $phone,
            'referring_site' => $domain,
            'subtotal_price' => $subtotal,
            'total_discounts' => $discount,
            'total_tax' => $tax,
           // 'total_shipping_price' => $shipping_cost,
            'total_price' => $total,
            'total_outstanding' => 0,
            'customer' => $customer,
            'billing_address' => $billing_address,
            'shipping_address' => $shipping_address,
            'line_items' => $line_items,
            'shipping_lines' => $shipping_lines,
            'metafields' => [
                ["key"=>"order_id", "value"=>$order_id, "type"=>"single_line_text_field", "namespace"=>"global"],
                ["key"=>"user_id", "value"=>$order_info['user_id'], "type"=>"single_line_text_field", "namespace"=>"global"],
            ],
        ];

        if( !empty($shopify_order_id) ){

            if( $isCancelled || $deleted ){
                
                $reason = $status == 'I' ? 'customer' : ($status=='F' || $status=='D' ? 'declined' : 'inventory');
                $res = $this->httpCall('POST','orders/'.$shopify_order_id.'/cancel',[],['reason'=>$reason]);

            } else {
                $order['id'] = $shopify_order_id;
                $res = $this->httpCall('PUT','orders/'.$shopify_order_id,[],['order'=>$order]);
            }
        } else {

            $order['name'] = $order_ref;
            $order['inventory_behaviour'] = "decrement_obeying_policy"; 
            
            $res = $this->httpCall('POST','orders',[],['order'=>$order]);
        }

        if( !empty($res['order']['id']) ){

            if( $isCancelled || $deleted )     
                $log = "Order Cancelled [".$res['order']['id']."]";

            else {

                $transaction_id = @$info['extra']['transaction_id'];
               
                if( $paid && empty($transaction_id) ){
            
                    $transaction = ["kind"=>"capture", "status"=>"success", "currency"=>CART_PRIMARY_CURRENCY, "gateway"=>"manual", "amount"=>$total,"source" => "external"];

                    $res_trans = $this->httpCall('POST','orders/'.$shopify_order_id.'/transactions',[],['transaction'=>$transaction]);
                    if( !empty($res_trans['transaction']['id']))
                        $transaction_id = $res_trans['transaction']['id'];
                }

                $this->saveSourceData('order', $order_id, $res['order']['id'], [
                    'price'=> $res['order']['total_price'],
                    'transaction_id' => $transaction_id,
                ], $order_ref);

                $log = "Order ".(!empty($shopify_order_id) ? "Updated" : "Created")." [".$res['order']['id']."]";
            }

            $shopify_order_id = @$res['order']['id'];

        } else {
            $error = @$res['errors'] ?: @$res['error'];
            if( !empty($error) && is_array($error) )
                $error = array_values($error)[0];

            if( empty($error) )
                $error = "Unable to ".($isCancelled ? "cancel" : (!empty($shopify_order_id) ? "update" : "create"));
            $log = is_array($error) ? json_encode($error) : $error;
        }

        if( !($isCancelled || $deleted) && !$error && !empty($shopify_order_id) )        
            $this->handleOrderShipments($order_id,$shopify_order_id,$order_ref,$products,$order,$order_info);

        return [$log,$error];
    }

    // process webhooks 

    protected function processWebhook($_data, $_headers){

        $type = $id = $data = $action = null;

        $id = @$_data['id'];
        $event = @$_headers['x-shopify-topic'];
        
        if( !empty($id) ){
            
            if( strpos($event,'orders/') !== false ){
                
                $order_id = (int)db_get_field("SELECT object_id FROM ?:cm_shop_connectors_source_data WHERE object_type = ?s AND shop_id=?i AND source_id = ?i", 'order', $this->shop_id, $id);
                
                if( !empty($order_id) ){

                    if( !empty($_data['cancelled_at']) ){

                        fn_change_order_status($order_id ,"I",'',true);
                        return true;

                    } else {

                        if( @$_data['fulfillment_status']=='fulfilled' && !empty($_data['fulfillments'][0]['id']) ){
                            $data = [
                                'carrier' => trim($_data['fulfillments'][0]['tracking_company']),
                                'tracking_number' =>trim($_data['fulfillments'][0]['tracking_number']),
                            ];
                            return $this->saveShipment($order_id,$data,$_data['fulfillments'][0]['id']);
                        }

                    }
                }

                return false;
            }

            if( strpos($event,'products/') !== false ){
                $type = 'product';
                $action = isset($_data['admin_graphql_api_id']) ? 'update' : 'delete';

                if( $action == 'update'){
                    if( !empty($_data['title']) ){

                        $collects = [];
                        $collect_list = $this->getAllData('collects');
                        if( !empty($collect_list) ){
                            foreach($collect_list as $col) {
                                $collects[$col['product_id']][] = 'collection:'.$col['collection_id'];
                            }
                        }
                        $_data['collections'] = (array)@$collects[$id];
                        if( !empty($_data['product_type']) )
                            $_data['collections'][] = 'product_type:'.strtolower($_data['product_type']);

                        $data = $this->parseRow($_data,[]);
                        if( empty($data['product_code']) )
                            $data = null;
                    }
                }
            }
        }
        
        return [$type, $id, $data, $action];
    }

    // create webhooks
    protected function saveWebhooks($url){
        $error = false;

        $res = $this->httpCall('GET','webhooks');
        $list = [];
        if( !empty($res['webhooks']) ){
            foreach($res['webhooks'] as $row){
                $list[md5($row['address'].$row['topic'])] = 1;
            }
        }

        if( !empty($res['error']) )
            $error = $res['error'];
        else {

            foreach($this->webhooks as $hook){

                $hash = md5($url.$hook);
                if( !isset($list[$hash]) ){

                    $_data = [ "topic" => $hook, "address"=> $url, "format" => "json"];
                    $res = $this->httpCall('POST','webhooks',[],['webhook'=>$_data]);
                    if( !empty($res['error']) || !empty($res['errors']) ){ 
                        $error = @$res['error'] ?: @$res['errors'];
                        if( is_array($error) ){
                            foreach($error as $k => $v){
                                if( $k == 'address')
                                    $k = "Website url"; 
                                $error = ucfirst($k)." ".(is_array($v) ? array_values($v)[0] : $v);
                                break;
                            }
                        }
                    }
                }    
            }
        }

        return $error;
    }

    // verify permissions
    private function verifyPermissions(){

    
        $permissions = [
            'read_product_listings' => 'Read Product Listings',
            'read_products' => 'Read Products',
            'read_inventory' => 'Read Inventory',
            'read_orders' => 'Read Orders',
            'write_orders' => 'Write Orders',
            'read_fulfillments' => 'Read Fulfillments',
            'write_fulfillments' => 'Write Fulfillments',
            'read_assigned_fulfillment_orders' => "Read Assigned Fulfillment Orders",
        ];

        $res = $this->httpCall('GET','https://'.str_replace(['http://','https://'],'',$this->shop_url).'/admin/oauth/access_scopes');
        $error = false;

        if( !empty($res['access_scopes']) ){
            $shop_prms = array_column((array)$res['access_scopes'],'handle');
            $missing = [];
            foreach($permissions as $ky => $lbl ){
                if( !in_array($ky, $shop_prms) )
                    $missing[] = $ky;
            }

            if( !empty($missing) )
                $error = 'Required permissions "<span style="font-style:italic; font-size:14px">'.implode(", ",array_unique($missing)).'</span>" are missing in Shopify App';
        }

        $this->error = $error;
        return !$error;
    }

    // get products
    protected function getProducts(){

        $list_type = @$this->settings['list_type'] ?: 'all';
        $list = [];
            
        $category_mapping = (array)$this->mapping;
        foreach( $category_mapping as $category => $enabled ){
            if( !(int)$enabled ) 
                unset($category_mapping[$category]);
        }

        if( empty($category_mapping) ){
            $this->error = "Please map atleast one category to import products";
            return [];
        }

        if( !$this->verifyPermissions() )
            return [];
            
        $products = [];
        $count = 0;
        $error = false;        

        foreach(array_keys($category_mapping) as $key){
            
            list($type,$id) = explode(':',$key);
            
            $this->error =false;
            $_products = [];

            if( $type == 'colllection' ) $type = 'collection';

            if( $type == 'collection' )
                $_products = $this->getAllData('products',['collection_id'=>$id]);

            if( $type == 'product_type')
                $_products = $this->getAllData('products',['product_type'=>$id]);

            if( $count == 0 && $this->error );
                $error = $this->error;

            if( !empty($_products) ){
                foreach($_products as $prd){
                    if( !empty($prd['id']) ){
                        $pid = $prd['id'];

                        if( isset($products[$pid]) )
                            $products[$pid]['collections'][] = $key;
                        else {
                            $prd['collections'][] = $key;
                            $products[$pid] = $prd;
                        }
                    }
                }
            }

            $count++;
            sleep(1);
            unset($_products);
        }

        $this->error = $error;
        if( !empty($products) )
            $this->error = false;

        foreach ($products as $row) { 
            if( !empty($row['id']) && !empty($row['title'])
                && ($list_type == 'all' || ($list_type == 'active' && $row['status'] == 'active') ) 
            ){
                $data = $this->parseRow($row,[]);
                if( !empty($data['product_code']) )
                    $list[$row['id']] = $data;
            }
        } 

        return $list;
    }

    // get categories
    protected function getCategories(){

        $list = [];

        $product_types = $this->getProductTypes();
        if( !$this->error ){

            foreach ($product_types as $key => $value){ 
                $list["product_type:".strtolower($value)] = $value." (Product Type)";
            }

            $smart_collections = $this->getAllData('smart_collections');
            if( !$this->error ){
                if( !empty($smart_collections) ){
                    foreach ($smart_collections as $c) {
                        $list["collection:".$c['id']] = $c['title']. " (Collection)";
                    }
                }
                $custom_collections = $this->getAllData('custom_collections');

                if( !empty($custom_collections) ){
                    foreach ($custom_collections as $c) {
                        $list["collection:".$c['id']] = $c['title']. " (Collection)";
                    }
                }
            }      
        }

        return $list;
    }
    
    // core 

    protected function getShopData($key,$product_code,$data,$old_id){
    
        if( !empty($data['main_image']) ) 
            $data['main_image'] = $this->loadImage($data['main_image'],true);
        
        if( !empty($data['additional_images']) )
            $data['additional_images'] = $this->loadImage($data['additional_images'],false);

        if( !empty($data['variations']) ){
            foreach($data['variations'] as &$var){
                if( !empty($var['main_image']) ) 
                    $var['main_image'] = $this->loadImage($var['main_image'],true);
                
                if( !empty($var['additional_images']) )
                    $var['additional_images'] = $this->loadImage($var['additional_images'],false);
            }
        }

        return $data;
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
                        'weight'=> $this->getWeight(@$var['weight'], @$var['weight_unit']),
                        'extra'=>[ 'id' => $var['product_id'], 'variant_id' => $var['id'] ],
                    );   
                }
            }
        }

        if( empty($parent_sku) )
            $parent_sku = $sku;
        
        return [$options,$combinations,$parent_sku];
    }
 
    protected function parseRow($data,$collects){
        
        $id = $data['id'];
        $variant = @$data['variants'][0];

        // categories
        $category_ids = [];        
        $pr_collects = (array)@$data['collections'];
     
        foreach($pr_collects as $col_id){
            list($ctype,$clid) = explode(":",$col_id);
            if( $ctype == 'product_type' ){
                $cat_id = (int)$this->getMapIdsCSCart('categories',trim($clid),'product_type');
                if( !empty($cat_id) )
                    $category_ids[] = $cat_id;
            }
        }

        foreach($pr_collects as $col_id){
            list($ctype,$clid) = explode(":",$col_id);
            if( $ctype == 'colllection' ){
                $cat_id = (int)$this->getMapIdsCSCart('categories',trim($clid),'colllection');
                if( !empty($cat_id) )
                    $category_ids[] = $cat_id;
            }
            if( $ctype == 'collection' ){
                $cat_id = (int)$this->getMapIdsCSCart('categories',trim($clid),'collection');
                if( !empty($cat_id) )
                    $category_ids[] = $cat_id;
            }
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
        $weight = $this->getWeight(@$variant['weight'], @$variant['weight_unit']);

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

    private function getAllData($path,$params = array(),$limit = 250){
        
        $path = trim($path);
        $list = array();
        $res_count = $this->httpCall('GET',$path.'/count',$params);

        if( isset($res_count['count']) ){
            if( $res_count['count'] > 0 ){
                
                $totalPages = ceil((int)$res_count['count']/$limit);
                    
                $count = 0;
                $loop = true;

                while ( $loop ) {
                            
                    $count++;
                    $loop = false; 

                    $params['limit'] = $limit;  
                    if( !empty($params['page_info']) )
                        unset($params['collection_id'],$params['product_type']);

                    $response = $this->httpCall('GET',$path,$params);                 

                    if( @$response['errors'] ){ 
                        if( $count == 1 )
                            $this->error = $response['errors'];
                        else
                            $this->error = false;  
                    }

                    if( !empty($response[$path]) ){
                        foreach ($response[$path] as $p) {
                            $list[] = $p;
                        }
                        if( !empty($this->reqHeaders['link']) ){
                            $_link = $this->parseNextLink($this->reqHeaders['link']);
                            if( $_link ){
                                $params['page_info'] = $_link['page_info'];
                                $loop = true;
                            }
                        }
                    } 
                }
            }
        } else {
            if( !$this->error )
                $this->error = @$res_count['errors'] ?: 'Unable to get data from Shopify !';
        }

        return $list;
    }

    private function getProductTypes(){

        $ths= $this;
        $list = $this->getCached("product_types", 30, function() use(&$ths) {
            $list = [];
            $_list = $ths->getAllData('products',['fields'=>'product_type']);
            if( !$ths->error ){
                foreach ($_list as $r) {
                    if( !empty($r['product_type']) ){
                        $k = trim(strtolower($r['product_type']));
                        $list[$k] = trim($r['product_type']);
                    }
                }
            }
            return $list;
        });

        $this->error = $ths->error;
        return $list;
    }

}
