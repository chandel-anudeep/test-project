<?php
/***************************************************************************
*                                                                          *
*   (c) 2024 CartModules                                                   *
*                                                                          *
* This  is  commercial  software,  only  users  who have purchased a valid *
* license  and  accept  to the terms of the  License Agreement can install *
* and use this program.                                                    *
*                                                                          *
****************************************************************************
* PLEASE READ THE FULL TEXT  OF THE SOFTWARE  LICENSE   AGREEMENT  IN  THE *
* "copyright.txt" FILE PROVIDED WITH THIS DISTRIBUTION PACKAGE.            *
****************************************************************************/

use ShopifyConnector\Shopify;
use Tygh\Registry;

if (!defined('BOOTSTRAP')) { die('Access denied'); }

$company_id = Registry::get('runtime.company_id');

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
	/* For Shopify */

	if($mode === "get_categories" && !empty($_REQUEST['category_mapping']) ) {
		$shopify = new Shopify($_REQUEST['company_id']);
		$shopify->saveConfig(['category_mapping' => json_encode($_REQUEST['category_mapping'])]);
		if(!empty($_REQUEST['return_url']))
			fn_redirect(fn_url('cm_vendor_signup.apply_for_vendor'));
		else
			$suffix = ".cm_integration";
	}	

	if($mode === "get_products" && !empty($_REQUEST['product_ids']) ) {
		$shopify = new Shopify($_REQUEST['company_id']);
		$shopify->saveConfig(['product_ids' => implode(',', $_REQUEST['product_ids'])]);
		if(!empty($_REQUEST['return_url']))
			fn_redirect(fn_url('cm_vendor_signup.apply_for_vendor'));
		else
			$suffix = ".cm_integration";
	}

	if($mode === "disconnect") {
		$shopify = new Shopify($_REQUEST['company_id']);
		$disconnect = $shopify->disconnect();
		$suffix = ".cm_integration";
	}

	//fn_redirect(fn_url('cm_vendor_signup.apply_for_vendor'));


	if($mode === "cm_onboarding") {

        if( $company_id && !empty($_REQUEST['cm_onboarding']))
            db_query("UPDATE ?:cm_seller_shopify_details SET cm_onboarding = 'Y' WHERE company_id = ?i", $company_id);

        $suffix = ".cm_integration";
    }

    if($mode === "connect_shopify") {
        $store = trim($_REQUEST['shop_name']);
        if(!empty($store)) {
            $api_key = Registry::get('addons.cm_vendor_signup_advance.api_key');
            $scopes = 'read_orders, read_products, unauthenticated_read_product_listings';
            $nonce = $company_id . '_' . uniqid();
            $redirect_uri = urlencode(fn_url('cm_shopify.connect'));
            $url = "https://{$store}/admin/oauth/authorize?client_id={$api_key}&scope={$scopes}&redirect_uri={$redirect_uri}&state={$nonce}";

            header("Location: {$url}");
            exit;
        } else
            $suffix = ".cm_integration";
    }

    if($mode === "shopify_integration") {

        if( $company_id && !empty($_REQUEST['cm_shopify_integration']))
            db_query("UPDATE ?:cm_seller_shopify_details SET shopify_integration = 'Y' WHERE company_id = ?i", $company_id);

        $suffix = ".cm_integration";
    }

    $url = !empty(trim($suffix)) ? trim($suffix) : fn_url('cm_shopify.cm_integration');

    return array(CONTROLLER_STATUS_OK, 'cm_shopify' . $url);

    /***** For Shopify end *****/
}

if( $mode == 'import' || $mode == 'sync' ){

	if( $mode == 'import' && empty($_REQUEST['company_id']) ){
		fn_redirect(fn_url('cm_shopify.cm_integration', 'A'));
	}
	$cond = "store_connect = 'Y'";

	$company_id = (int)@$_REQUEST['company_id'];
	if( !empty($company_id) )
		$cond .= " AND company_id = $company_id";

	$list = db_get_array("SELECT * FROM ?:cm_seller_shopify_details WHERE $cond");

	if( $mode == 'import' && empty($list) ){
		fn_redirect(fn_url('cm_shopify.cm_integration', 'A'));
	}

	$shops = [];
	$error = false;

	$emails_errors = [];
	foreach($list as $row){

		$_error = false;
		$shop = new Shopify($row);
		
		if( $shop->connected ){

			if( !empty($shop->config['category_mapping']) && !empty($shop->config['product_ids']) ){		
				$shops[$shop->company_id] = $shop;
			
			} else {
				if( empty($shop->config['product_ids']) )
					$_error = 'Please map at least one product';
				else
					$_error = 'Please map at least one shopify category with shop category';
			}
		}

		if( $mode == 'import' )
			$error = $_error;	
		else
			$emails_errors[$shop->company_id] = $_error;
	}

	if( $mode == 'import' && $error ){
		fn_set_notification("E", __("error"), $error);
		fn_redirect(fn_url('cm_shopify.cm_integration', 'A'));
	}

	$done = [];
	$error = false;
	foreach($shops as $company_id => $shop){

		$products = $shop->getProducts();
		if( $shop->error ){

			if( $mode == 'import' )
				$error = $shop->error;	
			else
				$emails_errors[$shop->company_id] = $shop->error;

		} else {

			if( !empty($products) ){

				db_query("UPDATE ?:products SET cm_shopify_sync = 'N' WHERE company_id = ?i AND cm_shopify_sync = 'Y'",$company_id);

				$done[$company_id] = $shop->saveProducts($products, $mode);

				$shop->saveConfig(['last_sync'=>time()]);

				db_query("UPDATE ?:products SET status = 'D' WHERE company_id = ?i AND cm_shopify_sync = 'N'",$company_id);
			}
		}
	}
	
	if( $mode == 'import' && $error ){
		fn_set_notification("E", __("error"), $error);
		fn_redirect(fn_url('cm_shopify.cm_integration', 'A'));
	}

	if( $mode == 'import'){
		$count = array_values($done)[0] ?? 0;
		fn_set_notification("N", __('notice'), "$count products synced successfully!");
		
   		fn_redirect(fn_url('cm_shopify.cm_integration', 'A'));
	} else {

		foreach($emails_errors as $company_id => $error){
			if($error) {
				$company_data = $company_id > 0 ? fn_get_company_data($company_id, DESCR_SL, []) : [];
				$company_data['resolve_url'] = fn_url('vendor.php?dispatch=cm_shopify.cm_integration');
				$company_data['support_url'] = fn_url('pages.view&page_id=30', 'C');

				$event_dispatcher = Tygh::$app['event.dispatcher'];
				$event_dispatcher->dispatch('cm_vendor_shopify_data_sync_error', ['company_data' => $company_data ]);
			}	
		}

		echo "Sync:\n<br>";
		foreach($done as $company_id => $count){
			echo "#$company_id : $count products synced<br>\n";
		}

		exit;
	}
}

if($mode === "get_categories") {

	$shopify = new Shopify($_REQUEST['company_id']);

	$collections = $shopify->getCollections();
	Tygh::$app['view']->assign('company_id', $_REQUEST['company_id']);
	Tygh::$app['view']->assign('collections', $collections);
	// Tygh::$app['view']->assign('categories', $categories);
	Tygh::$app['view']->assign('error', $shopify->error);
	Tygh::$app['view']->assign('mapping', $shopify->config['category_mapping'] ?? []);

}


if($mode === "get_products") {

	if(!empty($_REQUEST['remove']) && !empty($_REQUEST['company_id'])) {
		db_query("UPDATE ?:cm_seller_shopify_details SET product_ids = NULL WHERE company_id = ?i", $_REQUEST['company_id']);
	}

	$shopify = new Shopify($_REQUEST['company_id']);
	list($products,$total,$next_page) = $shopify->getCollectionProducts($_REQUEST['next'] ?? null);
	$list = [];

	if( !empty($next_page) ) 
	 	$next_page = fn_url('cm_shopify.get_products?company_id='.$_REQUEST['company_id'].'&next='.$next_page, 'A');
	
	foreach ($products as $row){

		$category = 'Woman Clothing';
		
		$_row = [
			'id' => $row['id'],
			'name' => $row['title'],
			'price' => $row['variants'][0]['price'],
			'image' => $row['image']['src'] ?? null,
			'vendor' => $row['vendor'] ?? null,
		];


		$list[] = '<div class="item-product">
			<input type="checkbox" name="product_ids[]" value="'.$_row['id'].'" '.(in_array($_row['id'],$shopify->config['product_ids']) ? 'checked' : '').' />
			<div class="img">
				<img src="'.$_row['image'].'" width=110/>
			</div>
			<div class="info">';

		if(empty(Registry::get('runtime.company_id')))
			$list[] = '<div class="cat">'.$row['categories']['cscart'].'</div>';

		$list[] = '<div class="price">'.$_row['price'].'</div>
				<div class="name">'.$_row['name'].'</div>
				<div class="vndr">by '.$_row['vendor'].'</div>
			</div>
		</div>';
	}

	if(!empty($next_page) && !empty($list)) {
		$list[] = '<div class="btn_next">
			<a class="btm-shopify-loadmore" data-href="'.$next_page.'">Load More</a>
		</div>';
	}

	if( !empty($list) )
		$list = implode(" ",$list);
	else
		$list = null;

	if( !empty($_REQUEST['next']) ){
		header("Content_type: application/json");
		echo json_encode([
			'products'=>$list,
			'error'=>$shopify->error,
			'next_page'=> $next_page
		]);
		exit;
	}

	Tygh::$app['view']->assign('company_id', $_REQUEST['company_id']);
	Tygh::$app['view']->assign('products', $list);
	Tygh::$app['view']->assign('error', $shopify->error);
	Tygh::$app['view']->assign('mapped_product_ids', $shopify->config['product_ids'] ?? []);
	Tygh::$app['view']->assign('next_page', $next_page);
}


if($mode === "connect"){
	if(!empty($_REQUEST['shop']) && !empty($_REQUEST['hmac']) && !empty($_REQUEST['code'])) {
		
		$parts = explode("_", $_REQUEST['state']);
			
		$company_id = (int)$parts[0];
		$code = trim($_REQUEST['code']);
		$store_url = $_REQUEST['shop'];

		$shopify = new Shopify($company_id);
		$connected = $shopify->connect($store_url,$code,$_REQUEST['hmac']);

		if( $connected) {
			fn_set_notification('N', __('notice'), 'Your Shopify store connected sucessfully.');
		} else {
			fn_set_notification('E', __('error'), $shopify->error ?: "Unable to connect with your shopify store");
		}
	}

	if(AREA == 'A')
		fn_redirect(fn_url('cm_shopify.cm_integration'));
	else
		fn_redirect(fn_url('cm_vendor_signup.apply_for_vendor'));
}


/*************Vendor Panel*****************/

$company_data = $company_id > 0 ? fn_get_company_data($company_id, DESCR_SL, []) : [];
$cm_shopify = @$company_data['cm_shopify'];
$shopify_details = $company_id > 0 ? db_get_row('SELECT * FROM ?:cm_seller_shopify_details WHERE company_id = ?i', $company_id) : [];

if($mode === "cm_onboarding"){
	if($shopify_details['cm_onboarding'] == 'Y')
		fn_redirect(fn_url('cm_shopify.cm_integration'));
}

if($mode === "cm_integration"){
	Tygh::$app['view']->assign('current_time', time());
	Tygh::$app['view']->assign('company_data', $company_data);
	Tygh::$app['view']->assign('cm_shopify', $cm_shopify);
	Tygh::$app['view']->assign('shopify_details', $shopify_details);
}

/************************/
