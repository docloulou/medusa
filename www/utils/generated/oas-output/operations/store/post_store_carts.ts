/**
 * @oas [post] /store/carts
 * operationId: PostCarts
 * summary: Create Cart
 * description: Create a cart.
 * x-authenticated: false
 * parameters:
 *   - name: expand
 *     in: query
 *     description: Comma-separated relations that should be expanded in the returned data.
 *     required: false
 *     schema:
 *       type: string
 *       title: expand
 *       description: Comma-separated relations that should be expanded in the returned data.
 *   - name: fields
 *     in: query
 *     description: Comma-separated fields that should be included in the returned
 *       data. if a field is prefixed with `+` it will be added to the default
 *       fields, using `-` will remove it from the default fields. without prefix
 *       it will replace the entire default fields.
 *     required: false
 *     schema:
 *       type: string
 *       title: fields
 *       description: Comma-separated fields that should be included in the returned
 *         data. if a field is prefixed with `+` it will be added to the default
 *         fields, using `-` will remove it from the default fields. without prefix
 *         it will replace the entire default fields.
 *   - name: offset
 *     in: query
 *     description: The number of items to skip when retrieving a list.
 *     required: false
 *     schema:
 *       type: number
 *       title: offset
 *       description: The number of items to skip when retrieving a list.
 *   - name: limit
 *     in: query
 *     description: Limit the number of items returned in the list.
 *     required: false
 *     schema:
 *       type: number
 *       title: limit
 *       description: Limit the number of items returned in the list.
 *   - name: order
 *     in: query
 *     description: The field to sort the data by. By default, the sort order is
 *       ascending. To change the order to descending, prefix the field name with
 *       `-`.
 *     required: false
 *     schema:
 *       type: string
 *       title: order
 *       description: The field to sort the data by. By default, the sort order is
 *         ascending. To change the order to descending, prefix the field name with
 *         `-`.
 * requestBody:
 *   content:
 *     application/json:
 *       schema:
 *         description: SUMMARY
 *         properties:
 *           region_id:
 *             type: string
 *             title: region_id
 *             description: The cart's region id.
 *           customer_id:
 *             type: string
 *             title: customer_id
 *             description: The cart's customer id.
 *           sales_channel_id:
 *             type: string
 *             title: sales_channel_id
 *             description: The cart's sales channel id.
 *           email:
 *             type: string
 *             title: email
 *             description: The cart's email.
 *             format: email
 *           currency_code:
 *             type: string
 *             title: currency_code
 *             description: The cart's currency code.
 *           shipping_address_id:
 *             type: string
 *             title: shipping_address_id
 *             description: The cart's shipping address id.
 *           billing_address_id:
 *             type: string
 *             title: billing_address_id
 *             description: The cart's billing address id.
 *           shipping_address:
 *             oneOf:
 *               - type: string
 *                 title: shipping_address
 *                 description: The cart's shipping address.
 *               - $ref: "#/components/schemas/CreateCartAddress"
 *           billing_address:
 *             oneOf:
 *               - type: string
 *                 title: billing_address
 *                 description: The cart's billing address.
 *               - $ref: "#/components/schemas/CreateCartAddress"
 *           metadata:
 *             type: object
 *             description: The cart's metadata.
 *           items:
 *             type: array
 *             description: The cart's items.
 *             items:
 *               $ref: "#/components/schemas/CreateCartCreateLineItem"
 *           promo_codes:
 *             type: array
 *             description: The cart's promo codes.
 *             items:
 *               type: string
 *               title: promo_codes
 *               description: The promo code's promo codes.
 * x-codeSamples:
 *   - lang: Shell
 *     label: cURL
 *     source: curl -X POST '{backend_url}/store/carts'
 * tags:
 *   - Carts
 * responses:
 *   "200":
 *     description: OK
 *     content:
 *       application/json:
 *         schema:
 *           $ref: "#/components/schemas/StoreCartResponse"
 *   "400":
 *     $ref: "#/components/responses/400_error"
 *   "401":
 *     $ref: "#/components/responses/unauthorized"
 *   "404":
 *     $ref: "#/components/responses/not_found_error"
 *   "409":
 *     $ref: "#/components/responses/invalid_state_error"
 *   "422":
 *     $ref: "#/components/responses/invalid_request_error"
 *   "500":
 *     $ref: "#/components/responses/500_error"
 * x-workflow: createCartWorkflow
 * 
*/

