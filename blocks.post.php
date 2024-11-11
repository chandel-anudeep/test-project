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

use Tygh\Registry;

if (!defined('BOOTSTRAP')) { die('Access denied'); }

$schema['cm_cart_tabs'] = array(
    'templates' => array(
        'addons/cm_pro_cart_check/blocks/cm_cart_tabs.tpl' => array()
    ),
    'content' => array(
        'faq_content' => array(
            'type'     => 'simple_text',
            'required' => true,
        ),
        'return_policies' => array(
            'type'     => 'simple_text',
            'required' => true,
        )
    ),
    'wrappers'      => 'blocks/wrappers',
    'multilanguage' => true,
    'cache'         => false,
);

$schema['cm_cart_page_scroller'] = array(
    'templates' => array(
        'addons/cm_pro_cart_check/blocks/cm_cart_page_scroller.tpl' => array(
            'settings' => array(
                'show_price' => array (
                    'type' => 'checkbox',
                    'default_value' => 'Y'
                ),
                'enable_quick_view' => array (
                    'type' => 'checkbox',
                    'default_value' => 'N'
                ),
                'not_scroll_automatically' => array (
                    'type' => 'checkbox',
                    'default_value' => 'N'
                ),
                'scroll_per_page' =>  array (
                    'type' => 'checkbox',
                    'default_value' => 'N'
                ),
                'speed' =>  array (
                    'type' => 'input',
                    'default_value' => 400
                ),
                'pause_delay' =>  array (
                    'type' => 'input',
                    'default_value' => 3
                ),
                'item_quantity' =>  array (
                    'type' => 'input',
                    'default_value' => 5
                ),
                'thumbnail_width' =>  array (
                    'type' => 'input',
                    'default_value' => 80
                ),
                'outside_navigation' => array (
                    'type' => 'checkbox',
                    'default_value' => 'Y'
                )
            ),
        )
    ),
    'wrappers'      => 'blocks/wrappers',
    'multilanguage' => true,
    'cache'         => false,
);

return $schema;