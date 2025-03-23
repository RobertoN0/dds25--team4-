
# ------------------------------------------
# Request Event
# ------------------------------------------
EVENT_SUBTRACT_STOCK            = "SubtractStock"            # Command to instruct Stock service to subtract items
EVENT_ADD_STOCK                 = "AddStock"                 # Command to instruct Stock service to add items
EVENT_FIND_ITEM                 = "FindItem"                 # Command to instruct Stock service to find a specific item
EVENT_PAY                       = "Pay"                      # Command to instruct Payment service to withdraw payment
EVENT_REFUND                    = "Refund"                   # Command to instruct Payment service to refund payment
EVENT_CHECKOUT_REQUESTED        = "CheckoutRequested"        # Initiates the SAGA when an Order requests a checkout

# ------------------------------------------
# Response Event
# ------------------------------------------
EVENT_STOCK_SUBTRACTED          = "StockSubtracted"          # Stock service confirms stock has been subtracted
EVENT_STOCK_ERROR               = "StockError"               # Stock service signals an error
EVENT_ITEM_FOUND                = "ItemFound"                # Stock service found item details
EVENT_ITEM_NOT_FOUND            = "ItemNotFound"             # Stock service could not find the requested item
EVENT_PAYMENT_SUCCESS           = "PaymentProcessed"         # Payment service signals successful payment
EVENT_PAYMENT_ERROR             = "PaymentError"             # Payment service signals a payment failure
EVENT_CHECKOUT_SUCCESS          = "CheckoutSuccess"          # Indicates a successful checkout (used by Orchestrator → Order)
EVENT_CHECKOUT_FAILED           = "CheckoutFailed"           # Event for an aborted/failed checkout
    
EVENT_REFUND_SUCCESS            =  "RefundProcessed"         # Payment service signals a successful refund
EVENT_REFUND_ERROR              =  "RefundError"             # Payment service signals a refund failure
EVENT_STOCK_COMPENSATED         = "StockCompensated"         # Stock service confirms stock compensation
EVENT_STOCK_COMPENSATION_FAILED = "StockCompensationFailed"  # Stock service signals compensation failure
