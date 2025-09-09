from datetime import datetime

import jsonschema


class JsonSchemaValidator():

    def __init__(self, schema):
        self.schema = schema
        self.validator = self.extend_validator()

    def extend_validator(self):
        BaseVal = jsonschema.Draft7Validator

        # Build a new type checker for datetime
        def is_datetime(checker, inst):
            try:
                if inst != None:
                    datetime.strptime(inst, '%Y%m%d%H%M%S')
                    return True
            except ValueError:
                return False

        date_check = BaseVal.TYPE_CHECKER.redefine(u'datetime', is_datetime)
        # Return a validator with the new type checker
        return jsonschema.validators.extend(BaseVal, type_checker=date_check)

    # def validate_instance(self, instance):
    #     return self.validator(schema=self.schema).validate(instance, self.schema)

    def validate_instance(self, instance):
        errors = list(self.validator(schema=self.schema).iter_errors(instance))
        if errors:
            raise Exception(" | ".join(e.message for e in errors))


"""
WPT schemas
"""
rop_tms_vehicle_order = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "TripDetailId": {"type": ["string", "null"]},
        "OrderNo": {"type": ["string", "null"]},
        "InformDate": {"type": ["datetime", "null"]},
        "OrderType": {"type": ["string", "null"]},
        "PackLocationId": {"type": ["string", "null"]},
        "PackLocationName": {"type": ["string", "null"]},
        "PackLocationAddress": {"type": ["string", "null"]},
        "PickLocationId": {"type": ["string", "null"]},
        "PickLocationName": {"type": ["string", "null"]},
        "PickLocationAddress": {"type": ["string", "null"]},
        "VehicleOrderNo": {"type": ["string", "null"]},
        "VehicleOrderDate": {"type": ["datetime", "null"]},
        "VehicleOrderById": {"type": ["string", "null"]},
        "VehicleOrderByName": {"type": ["string", "null"]},
        "VehicleBookNo": {"type": ["string", "null"]},
        "VehicleBookDate": {"type": ["datetime", "null"]},
        "VehicleBookById": {"type": ["string", "null"]},
        "VehicleBookByName": {"type": ["string", "null"]},
        "DriverInformTime": {"type": ["string", "null"]},
        "Driver": {"type": ["string", "null"]},
        "VehicleNo": {"type": ["string", "null"]},
        "VehicleType": {"type": ["string", "null"]},
        "VehicleTonnage": {"type": ["string", "null"]},
        "PickDate": {"type": ["datetime", "null"]},
        "TotalPackageQuantity": {"type": ["number", "null"]},
        "PickedPackageQuantity": {"type": ["number", "null"]},
        "PackDate": {"type": ["datetime", "null"]},
        "OrderStatus": {"type": ["string", "null"]},
        "CurrentTosId": {"type": ["string", "null"]},
        "OrderStationId": {"type": ["string", "null"]},
        "OrderStationName": {"type": ["string", "null"]},
        "TTStatusName": {"type": ["string", "null"]},
        "DrId": {"type": ["string", "null"]},
        "TransportCarrierId": {"type": ["string", "null"]},
        "VehicleSpecializedName": {"type": ["string", "null"]},
        "VehicleSpecializedId": {"type": ["string", "null"]},
        "BookStationId": {"type": ["string", "null"]},
        "BookStationName": {"type": ["string", "null"]},
        "BookingType": {"type": ["string", "null"]},
        "BookingTypeName": {"type": ["string", "null"]},
        "TransportTripAssignmentStatusName": {"type": ["string", "null"]},
        "TransportTripBookingStatusName": {"type": ["string", "null"]},
        "TransportTripAssignmentStatusId": {"type": ["string", "null"]},
        "TransportTripBookingStatusId": {"type": ["string", "null"]},
        "RouteType": {"type": ["string", "null"]},
        "RequestDate": {"type": ["datetime", "null"]}
    }
})

rop_winplus_order = JsonSchemaValidator(schema={
    "definitions": {
        "Items": {
            "properties": {
                "LineItem": {"type": ["string", "null"]},
                "ItemNo": {"type": ["string", "null"]},
                "ItemName": {"type": ["string", "null"]},
                "UnitCode": {"type": ["string", "null"]},
                "Itembarcode": {"type": ["string", "null"]},
                "Qty": {"type": ["number", "null"]},
                "Vat": {"type": ["number", "null"]},
                "TaxAmount": {"type": ["number", "null"]},
                "VinIdRedeem": {"type": ["number", "null"]},
                "VinIdEarn": {"type": ["number", "null"]},
                "UnitPrice": {"type": ["number", "null"]},
                "TotalAmount": {"type": ["number", "null"]},
                "MemberPointRedeemVAT": {"type": ["number", "null"]},
                "MemberPointEarnVat": {"type": ["number", "null"]},
            },
            "additionalProperties": False
        },
        "Payments":
            {
                "properties":
                    {
                        "PaymentTypeId": {"type": ["string", "null"]},
                        "PaymentTypeName": {"type": ["string", "null"]},
                        "Amount": {"type": ["number", "null"]},
                        "ReferenceCode": {"type": ["string", "null"]},
                        "ApprovalCode": {"type": ["string", "null"]},
                        "PaymentCardName": {"type": ["string", "null"]},
                        "VoucherCode": {"type": ["string", "null"]}
                    },
                "additionalProperties": False
            }
    },
    "type": "object",
    "required": ["BillCode", "BillDate", "PosCode", "StoreId", "SaleType", "MemberType", "TotalAmountItem", "Items",
                 "Payments"],
    "additionalProperties": False,
    "properties":
        {
            "BillCode": {"type": ["string", "null"]},
            "BillDate": {"type": ["string", "null"]},
            "PosCode": {"type": ["string", "null"]},
            "StoreId": {"type": ["string", "null"]},
            "StoreName": {"type": ["string", "null"]},
            "SaleType": {"type": ["string", "null"]},
            "CustomerName": {"type": ["string", "null"]},
            "TotalAmountItem": {"type": ["number", "null"]},
            "TotalCashAmount": {"type": ["number", "null"]},
            "TotalOtherAmount": {"type": ["number", "null"]},
            "MemberType": {"type": ["number", "null"]},
            "CustomerPhone": {"type": ["string", "null"]},
            "Items": {"type": "array", "items": {"$ref": "#/definitions/Items"}},
            "Payments": {"type": "array", "items": {"$ref": "#/definitions/Payments"}},
            "ProcDate": {"type": ["string", "null"]} #Added by CT
        }
})

"""
POS WCM schemas
"""

pos_wcm_saleout = JsonSchemaValidator(schema={
    "definitions":
        {
            "DiscountEntry":
                {
                    "properties":
                        {
                            "ReceiptNo": {"type": ["string", "null"]},
                            "LineNo": {"type": ["number", "null"]},
                            "TranNo": {"type": ["number", "null"]},
                            "ItemNo": {"type": ["string", "null"]},
                            "UOM": {"type": ["string", "null"]},
                            "OfferType": {"type": ["string", "null"]},
                            "OfferNo": {"type": ["string", "null"]},
                            "Quantity": {"type": ["number", "null"]},
                            "DiscountAmount": {"type": ["number", "null"]}
                        },
                    "additionalProperties": False
                },
            "TransPaymentEntry":
                {
                    "properties":
                        {
                            "IsOnline": {"type": ["boolean", "null"]},
                            "TenderType": {"type": ["string", "null"]},
                            "BankCardType": {"type": ["string", "null"]},
                            "ReferenceNo": {"type": ["string", "null"]},
                            "ApprovalCode": {"type": ["string", "null"]},
                            "AmountTendered": {"type": ["number", "null"]},
                            "CurrencyCode": {"type": ["string", "null"]},
                            "ReceiptNo": {"type": ["string", "null"]},
                            "ExchangeRate": {"type": ["number", "null"]},
                            "BankPOSCode": {"type": ["string", "null"]},
                            "LineNo": {"type": ["number", "null"]},
                            "AmountInCurrency": {"type": ["number", "null"]}
                        },
                    "additionalProperties": False
                },
            "TransLine":
                {
                    "properties":
                        {
                            "TranNo": {"type": ["number", "null"]},
                            "Barcode": {"type": ["string", "null"]},
                            "Article": {"type": ["string", "null"]},
                            "Uom": {"type": ["string", "null"]},
                            "Name": {"type": ["string", "null"]},
                            "POSQuantity": {"type": ["number", "null"]},
                            "Price": {"type": ["number", "null"]},
                            "Amount": {"type": ["number", "null"]},
                            "VATAmount": {"type": ["number", "null"]},
                            "Brand": {"type": ["string", "null"]},
                            "SerialNo": {"type": ["string", "null"]},
                            "DiscountEntry": {"type": "array", "items": {"$ref": "#/definitions/DiscountEntry"}}
                        },
                    "required": ["TranNo", "Article", "POSQuantity", "Amount"],
                    "additionalProperties": True
                }
        },

    "type": "object",
    "required": ["TransLine", "CalendarDay", "StoreCode", "PosNo", "ReceiptNo", "TranTime"],
    "additionalProperties": True,
    "properties":
        {
            "CalendarDay": {"type": ["string", "null"]},
            "StoreCode": {"type": ["string", "null"]},
            "PosNo": {"type": ["string", "null"]},
            "ReceiptNo": {"type": ["string", "null"]},
            "TranTime": {"type": ["string", "null"]},
            "MemberCardNo": {"type": ["string", "null"]},
            "VinidCsn": {"type": ["string", "null"]},
            "Header_ref_01": {"type": ["string", "null"]},
            "Header_ref_02": {"type": ["string", "null"]},
            "Header_ref_03": {"type": ["string", "null"]},
            "Header_ref_04": {"type": ["string", "null"]},
            "Header_ref_05": {"type": ["string", "null"]},
            "TransLine": {"type": "array", "items": {"$ref": "#/definitions/TransLine"}},
            "TransPaymentEntry": {"type": "array", "items": {"$ref": "#/definitions/TransPaymentEntry"}}
        }
})

"""
POS PLG topics
"""

pos_plg_saleout = JsonSchemaValidator(schema={
    "definitions":
        {
            "DiscountEntry":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "ParentLineId": {"type": ["number", "null"]},
                            "ParentLineNo": {"type": ["number", "null"]},
                            "LineId": {"type": ["number", "null"]},
                            "OfferNo": {"type": ["string", "null"]},
                            "OfferType": {"type": ["string", "null"]},
                            "Quantity": {"type": ["number", "null"]},
                            "DiscountAmount": {"type": ["number", "null"]},
                            "Note": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "Loyalty":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "ParentLineId": {"type": ["number", "null"]},
                            "LineId": {"type": ["number", "null"]},
                            "MemberCardNumber": {"type": ["string", "null"]},
                            "ClubCode": {"type": ["string", "null"]},
                            "LoyaltyPointsEarn": {"type": ["number", "null"]},
                            "LoyaltyPointsRedeem": {"type": ["number", "null"]}
                        },
                    "additionalProperties": False
                },
            "Items":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "LineId": {"type": ["number", "null"]},
                            "ParentLineId": {"type": ["number", "null"]},
                            "ItemNo": {"type": ["string", "null"]},
                            "ItemName": {"type": ["string", "null"]},
                            "Uom": {"type": ["string", "null"]},
                            "OldPrice": {"type": ["number", "null"]},
                            "UnitPrice": {"type": ["number", "null"]},
                            "Qty": {"type": ["number", "null"]},
                            "DiscountAmount": {"type": ["number", "null"]},
                            "LineAmount": {"type": ["number", "null"]},
                            "VatGroup": {"type": ["string", "null"]},
                            "VatPercent": {"type": ["number", "null"]},
                            "Note": {"type": ["string", "null"]},
                            "CupType": {"type": ["string", "null"]},
                            "Size": {"type": ["string", "null"]},
                            "IsTopping": {"type": ["boolean", "null"]},
                            "IsCombo": {"type": ["boolean", "null"]},
                            "ComboId": {"type": ["number", "null"]},
                            "ArticleType": {"type": ["string", "null"]},
                            "Barcode": {"type": ["string", "null"]},
                            "IsLoyalty": {"type": ["boolean", "null"]}
                        },
                    "additionalProperties": False
                },
            "Payments":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "LineId": {"type": ["number", "null"]},
                            "PaymentMethod": {"type": ["string", "null"]},
                            "CurrencyCode": {"type": ["string", "null"]},
                            "ExchangeRate": {"type": ["number", "null"]},
                            "AmountTendered": {"type": ["number", "null"]},
                            "AmountInCurrency": {"type": ["number", "null"]},
                            "TransactionNo": {"type": ["string", "null"]},
                            "ApprovalCode": {"type": ["string", "null"]},
                            "TraceCode": {"type": ["string", "null"]},
                            "ReferenceId": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "CouponEntry":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "ParentLineId": {"type": ["number", "null"]},
                            "LineId": {"type": ["number", "null"]},
                            "Description": {"type": ["string", "null"]},
                            "OfferNo": {"type": ["string", "null"]},
                            "OfferType": {"type": ["string", "null"]},
                            "Barcode": {"type": ["string", "null"]}
                        },
                    "additionalProperties": True
                }
        },

    "type": "object",
    "required": ["OrderNo", "OrderDate", "StoreNo", "PosNo", "TransactionType", "Items", "Payments"],
    "additionalProperties": True,
    "properties":
        {
            "OrderNo": {"type": ["string", "null"]},
            "OrderDate": {"type": ["string", "null"]},
            "StoreNo": {"type": ["string", "null"]},
            "PosNo": {"type": ["string", "null"]},
            "CustName": {"type": ["string", "null"]},
            "Note": {"type": ["string", "null"]},
            "TransactionType": {"type": ["number", "null"]},
            "SalesType": {"type": ["string", "null"]},
            "OrderTime": {"type": ["string", "null"]},
            "ReturnedOrderNo": {"type": ["string", "null"]},
            "IsRetry": {"type": ["boolean", "null"]},
            "Items": {"type": "array", "items": {"$ref": "#/definitions/Items"}},
            "Payments": {"type": "array", "items": {"$ref": "#/definitions/Payments"}},
            "Loyalty": {"type": ["array", "null"], "items": {"$ref": "#/definitions/Loyalty"}},
            "DiscountEntry": {"type": ["array", "null"], "items": {"$ref": "#/definitions/DiscountEntry"}},
            "CouponEntry": {"type": ["array", "null"], "items": {"$ref": "#/definitions/CouponEntry"}}
        }
})

pos_wcm_saleout_plg = JsonSchemaValidator(schema={
    "definitions":
        {
            "TransPaymentEntry":
                {
                    "properties":
                        {
                            "LineNo": {"type": ["number", "null"]},
                            "TenderType": {"type": ["string", "null"]},
                            "CurrencyCode": {"type": ["string", "null"]},
                            "ExchangeRate": {"type": ["number", "null"]},
                            "PaymentAmount": {"type": ["number", "null"]},
                            "ReferenceNo": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "TransLine":
                {
                    "properties":
                        {
                            "LineNo": {"type": ["number", "null"]},
                            "ParentLineNo": {"type": ["number", "null"]},
                            "ItemNo": {"type": ["string", "null"]},
                            "ItemName": {"type": ["string", "null"]},
                            "Uom": {"type": ["string", "null"]},
                            "Quantity": {"type": ["number", "null"]},
                            "UnitPrice": {"type": ["number", "null"]},
                            "DiscountAmount": {"type": ["number", "null"]},
                            "VATPercent": {"type": ["number", "null"]},
                            "LineAmount": {"type": ["number", "null"]},
                            "MemberPointsEarn": {"type": ["number", "null"]},
                            "MemberPointsRedeem": {"type": ["number", "null"]},
                            "CupType": {"type": ["string", "null"]},
                            "Size": {"type": ["string", "null"]},
                            "IsTopping": {"type": ["boolean", "null"]},
                            "IsCombo": {"type": ["boolean", "null"]},
                            "ScanTime": {"type": ["string", "null"]},
                            "TransDiscountEntry": {"type": ["string", "null"]}
                        },
                    "required": ["LineNo", "ParentLineNo", "ItemNo", "ItemName"],
                    "additionalProperties": False
                }
        },

    "type": "object",
    "required": ["StoreNo", "OrderNo", "OrderDate", "SaleType", "TransactionType", "MemberCardNo", "SalesStoreNo",
                 "SalesPosNo", "TransLine"],
    "additionalProperties": False,
    "properties":
        {
            "StoreNo": {"type": ["string", "null"]},
            "OrderNo": {"type": ["string", "null"]},
            "OrderDate": {"type": ["string", "null"]},
            "SaleType": {"type": ["string", "null"]},
            "TransactionType": {"type": ["string", "null"]},
            "MemberCardNo": {"type": ["string", "null"]},
            "SalesStoreNo": {"type": ["string", "null"]},
            "SalesPosNo": {"type": ["string", "null"]},
            "RefNo": {"type": ["string", "null"]},
            "TransLine": {"type": "array", "items": {"$ref": "#/definitions/TransLine"}},
            "TransPaymentEntry": {"type": "array", "items": {"$ref": "#/definitions/TransPaymentEntry"}},
            "ProcDate": {"type": ["string", "null"]} #Added by CT
        }
})

pos_wcm_saleout_cancel = JsonSchemaValidator(schema={
    "definitions":
        {
            "VoidHeader":
                {
                    "properties":
                        {
                            "OrderNo": {"type": ["string", "null"]},
                            "OrderDate": {"type": ["string", "null"]},
                            "CustomerNo": {"type": ["string", "null"]},
                            "CustomerName": {"type": ["string", "null"]},
                            "ZoneNo": {"type": ["string", "null"]},
                            "ShipToAddress": {"type": ["string", "null"]},
                            "StoreNo": {"type": ["string", "null"]},
                            "POSTerminalNo": {"type": ["string", "null"]},
                            "ShiftNo": {"type": ["string", "null"]},
                            "CashierID": {"type": ["string", "null"]},
                            "AmountInclVAT": {"type": ["number", "null"]},
                            "UserID": {"type": ["string", "null"]},
                            "PrepaymentAmount": {"type": ["number", "null"]},
                            "DeliveringMethod": {"type": ["number", "null"]},
                            "TanencyNo": {"type": ["string", "null"]},
                            "SalesIsReturn": {"type": ["number", "null"]},
                            "MemberCardNo": {"type": ["string", "null"]},
                            "ReturnedOrderNo": {"type": ["string", "null"]},
                            "TransactionType": {"type": ["number", "null"]},
                            "PrintedNumber": {"type": ["number", "null"]},
                            "LastUpdated": {"type": ["string", "null"]},
                            "UserVoid": {"type": ["string", "null"]},
                            "DocumentType": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "VoidLine":
                {
                    "properties":
                        {
                            "ScanTime": {"type": ["string", "null"]},
                            "OrderNo": {"type": ["string", "null"]},
                            "LineType": {"type": ["number", "null"]},
                            "LocationCode": {"type": ["string", "null"]},
                            "ItemNo": {"type": ["string", "null"]},
                            "Description": {"type": ["string", "null"]},
                            "UnitOfMeasure": {"type": ["string", "null"]},
                            "Quantity": {"type": ["number", "null"]},
                            "UnitPrice": {"type": ["number", "null"]},
                            "DiscountAmount": {"type": ["number", "null"]},
                            "LineAmountIncVAT": {"type": ["number", "null"]},
                            "StaffID": {"type": ["string", "null"]},
                            "VATCode": {"type": ["string", "null"]},
                            "DeliveringMethod": {"type": ["number", "null"]},
                            "Barcode": {"type": ["string", "null"]},
                            "DivisionCode": {"type": ["string", "null"]},
                            "SerialNo": {"type": ["string", "null"]},
                            "OrigOrderNo": {"type": ["string", "null"]},
                            "LotNo": {"type": ["string", "null"]},
                            "ArticleType": {"type": ["string", "null"]},
                            "LastUpdated": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                }
        },
    "type": "object",
    "required": ["TransVoidHeader", "TransVoidLine"],
    "additionalProperties": False,
    "properties":
        {
            "TransVoidHeader": {"type": "array", "items": {"$ref": "#/definitions/VoidHeader"}},
            "TransVoidLine": {"type": "array", "items": {"$ref": "#/definitions/VoidLine"}},
            "ProcDate": {"type": ["string", "null"]} #Added by CT
        }
})

sap_hcm_hr = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "id": {"type": ["number", "null"]},
        "year": {"type": ["string", "null"]},
        "data": {"type": ["string", "null"]},
        "employeeID": {"type": ["string", "null"]},
        "name": {"type": ["string", "null"]},
        "gender": {"type": ["string", "null"]},
        "dob": {"type": ["string", "null"]},
        "age": {"type": ["string", "null"]},
        "bu": {"type": ["string", "null"]},
        "entity": {"type": ["string", "null"]},
        "department": {"type": ["string", "null"]},
        "position": {"type": ["string", "null"]},
        "rank": {"type": ["string", "null"]},
        "rankGroup": {"type": ["string", "null"]},
        "function": {"type": ["string", "null"]},
        "functionGroup": {"type": ["string", "null"]},
        "makeNonMake": {"type": ["string", "null"]},
        "startDate": {"type": ["string", "null"]},
        "location": {"type": ["string", "null"]},
        "contract": {"type": ["string", "null"]},
        "educationDegree": {"type": ["string", "null"]},
        "ageGroupe": {"type": ["string", "null"]},
        "lengthOfServiceGroup": {"type": ["string", "null"]},
        "f9box": {"type": ["string", "null"]},
        "directIndirect": {"type": ["string", "null"]},
        "processingMonth": {"type": ["number", "null"]},
        "processingYear": {"type": ["number", "null"]},
        "createdBy": {"type": ["string", "null"]},
        "createdDate": {"type": ["string", "null"]},
        "fileName": {"type": ["string", "null"]},
    },
    "required": [
        "id",
        "year",
        "employeeID",
        "gender",
        "dob",
        "bu",
        "entity",
        "rank",
        "rankGroup",
        "function",
        "functionGroup",
        "makeNonMake",
        "startDate",
        "contract",
        "educationDegree",
        "ageGroupe",
        "lengthOfServiceGroup",
        "directIndirect"
    ],
    "additionalProperties": True
})

sap_hcm_turnover = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "id": {"type": ["number", "null"]},
        "year": {"type": ["string", "null"]},
        "employeeID": {"type": ["string", "null"]},
        "firstName": {"type": ["string", "null"]},
        "lastName": {"type": ["string", "null"]},
        "fullName": {"type": ["string", "null"]},
        "gender": {"type": ["string", "null"]},
        "dob": {"type": ["string", "null"]},
        "age": {"type": ["string", "null"]},
        "entity": {"type": ["string", "null"]},
        "department": {"type": ["string", "null"]},
        "onboarddate": {"type": ["string", "null"]},
        "title": {"type": ["string", "null"]},
        "rank": {"type": ["string", "null"]},
        "function": {"type": ["string", "null"]},
        "stafftype": {"type": ["string", "null"]},
        "onboard": {"type": ["string", "null"]},
        "onboardOfficial": {"type": ["string", "null"]},
        "effectiveOff": {"type": ["string", "null"]},
        "contractType": {"type": ["string", "null"]},
        "numberOfWorkingYears": {"type": ["string", "null"]},
        "numberOfWorkingMonths": {"type": ["string", "null"]},
        "localExpat": {"type": ["string", "null"]},
        "typeOfLeaving": {"type": ["string", "null"]},
        "f9box": {"type": ["string", "null"]},
        "directIndirect": {"type": ["string", "null"]},
        "countTurnover": {"type": ["string", "null"]},
        "bu": {"type": ["string", "null"]},
        "voluntaryInvoluntary": {"type": ["string", "null"]},
        "processingMonth": {"type": ["number", "null"]},
        "processingYear": {"type": ["number", "null"]},
        "createdBy": {"type": ["string", "null"]},
        "createdDate": {"type": ["string", "null"]},
        "fileName": {"type": ["string", "null"]}
    },
    "required": [
        "id",
        "year",
        "employeeID",
        "firstName",
        "lastName",
        "fullName",
        "gender",
        "dob",
        "age",
        "entity",
        "department",
        "onboarddate",
        "title",
        "rank",
        "function",
        "stafftype",
        "onboard",
        "onboardOfficial",
        "effectiveOff",
        "contractType",
        "numberOfWorkingYears",
        "numberOfWorkingMonths",
        "localExpat",
        "typeOfLeaving",
        "f9box",
        "directIndirect",
        "countTurnover",
        "bu",
        "voluntaryInvoluntary",
        "processingMonth",
        "processingYear",
        "createdBy",
        "createdDate",
        "fileName"
    ]
})

sap_hcm_training = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "id": {"type": ["number", "null"]},
        "year": {"type": ["string", "null"]},
        "bu": {"type": ["string", "null"]},
        "entity": {"type": ["string", "null"]},
        "function": {"type": ["string", "null"]},
        "employeeID": {"type": ["string", "null"]},
        "fullName": {"type": ["string", "null"]},
        "rank": {"type": ["string", "null"]},
        "position": {"type": ["string", "null"]},
        "lineManager": {"type": ["string", "null"]},
        "hrbp": {"type": ["string", "null"]},
        "courseID": {"type": ["string", "null"]},
        "course": {"type": ["string", "null"]},
        "organizer": {"type": ["string", "null"]},
        "type": {"type": ["string", "null"]},
        "supplier": {"type": ["string", "null"]},
        "hour": {"type": ["string", "null"]},
        "trainingCost": {"type": ["string", "null"]},
        "directIndirect": {"type": ["string", "null"]},
        "processingMonth": {"type": ["number", "null"]},
        "processingYear": {"type": ["number", "null"]},
        "createdBy": {"type": ["string", "null"]},
        "createdDate": {"type": ["string", "null"]},
        "fileName": {"type": ["string", "null"]}
    },
    "required": [
        "id",
        "year",
        "bu",
        "entity",
        "function",
        "employeeID",
        "fullName",
        "rank",
        "position",
        "lineManager",
        "hrbp",
        "courseID",
        "course",
        "organizer",
        "type",
        "supplier",
        "hour",
        "trainingCost",
        "directIndirect",
        "processingMonth",
        "processingYear",
        "createdBy",
        "createdDate",
        "fileName"
    ]
})


sap_hcm_revenue_profit = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "id": {"type": ["number", "null"]},
        "year": {"type": ["string", "null"]},
        "bu": {"type": ["string", "null"]},
        "revenue": {"type": ["string", "null"]},
        "profit": {"type": ["string", "null"]},
        "hrcost": {"type": ["string", "null"]},
        "processingMonth": {"type": ["number", "null"]},
        "processingYear": {"type": ["number", "null"]},
        "createdBy": {"type": ["string", "null"]},
        "createdDate": {"type": ["string", "null"]},
        "fileName": {"type": ["string", "null"]},
    },
    "required": [
        "id",
        "year",
        "bu",
        "revenue",
        "profit",
        "hrcost",
        "processingMonth",
        "processingYear",
        "createdBy",
        "createdDate",
        "fileName"
    ]
})


supra_f_inventory = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "WH_CODE": {"type": "string"},
        "WH_SITE_ID": {"type": "string"},
        "CLIENT_CODE": {"type": "string"},
        "SKU": {"type": "string"},
        "SFT_INSTOCK_QTY": {"type": "number"},
        "SFT_AVAILABLE_QTY": {"type": "number"},
        "SFT_AFTER_PACKING_QTY": {"type": "number"},
        "SFT_PENDING_IN_QTY": {"type": "number"},
        "SFT_NOT_SYNC_SAP_PENDING_IN_QTY": {"type": "number"},
        "SFT_PO_PROCESSING_QTY": {"type": "number"},
        "RKT_STO_INSTOCK_QTY": {"type": "number"},
        "RKT_AVAILABLE_QTY": {"type": "number"},
        "SFT_LOST_QTY": {"type": "number"},
        "SFT_PHYSICAL_LOST_QTY": {"type": "number"},
        "SFT_PICKABLE_QTY": {"type": "number"},
        "SFT_DAMAGE_QTY": {"type": "number"},
        "SFT_OUT_OF_DATE_QTY": {"type": "number"},
        "SFT_NEARING_EXPIRATION_DATE_QTY": {"type": "number"},
        "SFT_RETURN_TO_VENDOR_QTY": {"type": "number"},
        "UOM": {"type": "string"},
        "CONDITION_TYPE": {"type": "string"},
        "CALDAY": {"type": "datetime"},
        "PROC_DAY": {"type": "datetime"},
        "TYPE": {"type": "string"}
    },
    "required": ["WH_CODE", "WH_SITE_ID", "CLIENT_CODE", "SKU",
                 "SFT_INSTOCK_QTY", "SFT_AVAILABLE_QTY", "SFT_DAMAGE_QTY", "SFT_OUT_OF_DATE_QTY",
                 "SFT_NEARING_EXPIRATION_DATE_QTY",
                 "SFT_RETURN_TO_VENDOR_QTY", "SFT_PENDING_IN_QTY", "SFT_PO_PROCESSING_QTY", "RKT_STO_INSTOCK_QTY",
                 "RKT_AVAILABLE_QTY",
                 "SFT_LOST_QTY", "SFT_PHYSICAL_LOST_QTY", "SFT_PICKABLE_QTY", "SFT_NOT_SYNC_SAP_PENDING_IN_QTY", "UOM",
                 "CONDITION_TYPE",
                 "CALDAY", "PROC_DAY", "TYPE"]
})

supra_f_store_po_pending_inv = JsonSchemaValidator(schema={
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "DC_SITE_ID": {"type": "string"},
        "STORE_ID": {"type": "string"},
        "TYPE": {"type": "string"},
        "SKU": {"type": "string"},
        "CLIENT_CODE": {"type": "string"},
        "SFT_SO_PROCESSING_QTY": {"type": "number"},
        "RKT_STO_INSTOCK_QTY": {"type": "number"},
        "UOM": {"type": "string"},
        "CONDITION_TYPE": {"type": "string"},
        "CALDAY": {"type": "datetime"},
        "PROC_DAY": {"type": "datetime"}
    },
    "required": ["STORE_ID", "TYPE", "CLIENT_CODE", "SKU", "RKT_STO_INSTOCK_QTY",
                 "UOM", "CONDITION_TYPE", "CALDAY", "PROC_DAY", "SFT_SO_PROCESSING_QTY"]
})

rop_good_movement = JsonSchemaValidator(schema={
    "definitions":
        {
            "Header":
                {
                    "properties":
                        {
                            "DocNum": {"type": ["string", "null"]},
                            "TransactionType": {"type": ["string", "null"]},
                            "PostingDate": {"type": ["string", "null"]},
                            "EntryDate": {"type": ["string", "null"]},
                            "EntryTime": {"type": ["string", "null"]},
                            "UserName": {"type": ["string", "null"]},
                            "ReferenceDocNum": {"type": ["string", "null"]},
                            "DocHeaderTXT": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "Item":
                {
                    "properties":
                        {
                            "DocNum": {"type": ["string", "null"]},
                            "DocItem": {"type": ["string", "null"]},
                            "ArticleNumber": {"type": ["string", "null"]},
                            "Site": {"type": ["string", "null"]},
                            "StorageLocation": {"type": ["string", "null"]},
                            "MovementType": {"type": ["string", "null"]},
                            "StockType": {"type": ["string", "null"]},
                            "SpecialStockIndicator": {"type": ["string", "null"]},
                            "Vendor": {"type": ["string", "null"]},
                            "Quantity": {"type": ["string", "null"]},
                            "Unit": {"type": ["string", "null"]},
                            "QuantityPO": {"type": ["string", "null"]},
                            "OrderPriceUnit": {"type": ["string", "null"]},
                            "PO_Number": {"type": ["string", "null"]},
                            "PO_Item": {"type": ["string", "null"]},
                            "DeliveryCompletedIndicator": {"type": ["string", "null"]},
                            "MovementIndicator": {"type": ["string", "null"]},
                            "RefDoc": {"type": ["string", "null"]},
                            "RefDocItem": {"type": ["string", "null"]},
                            "MoveMat": {"type": ["string", "null"]},
                            "MovePlant": {"type": ["string", "null"]},
                            "MoveStLog": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                }
        },
    "type": "object",
    "required": ["Header", "Items"],
    "additionalProperties": False,
    "properties":
        {
            "Header": {"type": "object", "items": {"$ref": "#/definitions/Header"}},
            "Items": {"type": "array", "items": {"$ref": "#/definitions/Item"}},
            "ItemsSpecified": {"type": ["boolean", "null"]},
            "ProcDate": {"type": ["string", "null"]} #Added by CT
        }
})

supra_d_sto_header_spr = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "STO_CODE": {"type": ["string", "null"]},
        "ROCKET_CODE": {"type": ["string", "null"]},
        "CLIENT_CODE": {"type": ["string", "null"]},
        "WH_CODE": {"type": ["string", "null"]},
        "WH_SITE_ID": {"type": ["string", "null"]},
        "EXTERNAL_CODE": {"type": ["string", "null"]},
        "PO_CODE": {"type": ["string", "null"]},
        "PROMOTION_CODE": {"type": ["string", "null"]},
        "TYPE": {"type": ["string", "null"]},
        "REQUEST_TYPE": {"type": ["string", "null"]},
        "STORE_SITE_ID": {"type": ["string", "null"]},
        "STORE_NAME": {"type": ["string", "null"]},
        "FULL_ADDRESS": {"type": ["string", "null"]},
        "ADDR_REGION": {"type": ["string", "null"]},
        "ADDR_PROVINCE": {"type": ["string", "null"]},
        "ADDR_DISTRICT": {"type": ["string", "null"]},
        "ADDR_WARD": {"type": ["string", "null"]},
        "CONTACT_NAME": {"type": ["string", "null"]},
        "CONTACT_PHONE": {"type": ["string", "null"]},
        "CREATED_DATE": {"type": ["string", "null"]},
        "CANCELED_DATE": {"type": ["string", "null"]},
        "CANCELLATION_REASON": {"type": ["string", "null"]},
        "TOTAL_WEIGHT": {"type": ["number", "null"]},
        "TOTAL_VOLUME": {"type": ["number", "null"]},
        "TOTAL_UNITS": {"type": ["number", "null"]},
        "STO_PICKING_TYPE": {"type": ["string", "null"]},
        "CONDITION_TYPE": {"type": ["string", "null"]},
        "SOURCE": {"type": ["string", "null"]},
        "STATUS": {"type": ["string", "null"]},
        "CALDAY": {"type": ["string", "null"]},
        "PROC_DATE": {"type": ["string", "null"]}
    },
    "required": ["STO_CODE", "ROCKET_CODE", "CLIENT_CODE", "WH_CODE", "WH_SITE_ID", "EXTERNAL_CODE", "PO_CODE",
                 "PROMOTION_CODE", "TYPE", "REQUEST_TYPE", "STORE_SITE_ID", "STORE_NAME", "FULL_ADDRESS", "ADDR_REGION",
                 "ADDR_PROVINCE", "ADDR_DISTRICT", "ADDR_WARD", "CONTACT_NAME", "CONTACT_PHONE", "CREATED_DATE",
                 "CANCELED_DATE", "CANCELLATION_REASON", "TOTAL_WEIGHT", "TOTAL_VOLUME", "TOTAL_UNITS",
                 "STO_PICKING_TYPE", "CONDITION_TYPE", "SOURCE", "STATUS", "CALDAY"]
})

supra_f_sto_detail_spr = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "STO_CODE": {"type": ["string", "null"]},
        "SKU": {"type": ["string", "null"]},
        "CLIENT_CODE": {"type": ["string", "null"]},
        "SKU_NAME": {"type": ["string", "null"]},
        "QTY": {"type": ["number", "null"]},
        "UOM": {"type": ["string", "null"]},
        "BASE_QTY": {"type": ["number", "null"]},
        "BASE_UOM": {"type": ["string", "null"]},
        "CALDAY": {"type": ["string", "null"]},
        "PROC_DATE": {"type": ["string", "null"]}
    },
    "required": ["STO_CODE", "SKU", "CLIENT_CODE", "SKU_NAME", "QTY", "UOM", "BASE_QTY", "BASE_UOM", "CALDAY",
                 "PROC_DATE"]
})

supra_f_sto_to_so_detail_spr = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "STO_CODE": {"type": ["string", "null"]},
        "SO_CODE": {"type": ["string", "null"]},
        "LINE_ITEM_NUMBER": {"type": ["string", "null"]},
        "SKU": {"type": ["string", "null"]},
        "CLIENT_CODE": {"type": ["string", "null"]},
        "SKU_NAME": {"type": ["string", "null"]},
        "QTY": {"type": ["number", "null"]},
        "UOM": {"type": ["string", "null"]},
        "CALDAY": {"type": ["string", "null"]},
        "PROC_DATE": {"type": ["string", "null"]}
    },
    "required": ["STO_CODE", "SO_CODE", "LINE_ITEM_NUMBER", "SKU", "CLIENT_CODE", "SKU_NAME", "QTY", "UOM", "CALDAY",
                 "PROC_DATE"]
})

supra_f_productivity = JsonSchemaValidator(schema={
    "properties": {
        "ClientCode": {"type": ["string", "null"]},
        "WarehouseCode": {"type": ["string", "null"]},
        "WarehouseSiteId": {"type": ["string", "null"]},
        "ObjectCode": {"type": ["string", "null"]},
        "RefCode": {"type": ["string", "null"]},
        "JobType": {"type": ["string", "null"]},
        "IsEven": {"type": ["boolean", "null"]},
        "Employee": {"type": ["string", "null"]},
        "EmployeeName": {"type": ["string", "null"]},
        "StartTime": {"type": ["string", "null"]},
        "EndTime": {"type": ["string", "null"]},
        "TotalSKU": {"type": ["number", "null"]},
        "TotalSKUProcessed": {"type": ["number", "null"]},
        "TotalUnit": {"type": ["number", "null"]},
        "TotalUnitProcessed": {"type": ["number", "null"]}
    },
    "required": [
        "ClientCode",
        "WarehouseCode",
        "WarehouseSiteId",
        "ObjectCode",
        "RefCode",
        "JobType",
        "IsEven",
        "Employee",
        "EmployeeName",
        "StartTime",
        "EndTime",
        "TotalSKU",
        "TotalSKUProcessed",
        "TotalUnit",
        "TotalUnitProcessed"
    ],
    "title": "Schema for SFT PRODUCTIVITY",
    "type": "object",
    "additionalProperties": True,
})

SUPRA_RECONCILE = JsonSchemaValidator(schema={
    "type": "object",
    "properties": {
        "DATA": {"type": ["string", "null"]},
        "TOTAL_RECORD": {"type": ["number", "null"]},
        "PROC_DATE": {"type": ["string", "null"]},
        "TYPE": {"type": ["string", "null"]}
    },
    "required": ["DATA", "TOTAL_RECORD", "PROC_DATE", "TYPE"]
})

# For CAPILLARY
cap_customer_add = JsonSchemaValidator(
    schema={"definitions": {}, "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://example.com/object1701759644.json", "type": ["object", "null"],
            "properties": {"eventName": {"$id": "#root/eventName", "type": ["string", "null"]},
                           "eventId": {"$id": "#root/eventId", "type": ["string", "null"]},
                           "orgId": {"$id": "#root/orgId", "type": ["integer", "null"]},
                           "refId": {"$id": "#root/refId", "type": ["string", "null"]},
                           "apiRequestId": {"$id": "#root/apiRequestId", "type": ["string", "null"]},
                           "createdAt": {"$id": "#root/createdAt", "type": ["integer", "null"]},
                           "data": {"$id": "#root/data", "type": ["object", "null"], "properties": {
                               "loyaltyType": {"$id": "#root/data/loyaltyType", "type": ["string", "null"]},
                               "source": {"$id": "#root/data/source", "type": ["string", "null"]},
                               "accountId": {"$id": "#root/data/accountId", "type": ["string", "null"]},
                               "firstName": {"$id": "#root/data/firstName", "type": ["string", "null"]},
                               "enteredAt": {"$id": "#root/data/enteredAt", "type": ["integer", "null"]},
                               "enteredBy": {"$id": "#root/data/enteredBy", "type": ["object", "null"], "properties": {
                                   "id": {"$id": "#root/data/enteredBy/id", "type": ["integer", "null"]},
                                   "till": {"$id": "#root/data/enteredBy/till", "type": ["object", "null"],
                                            "properties": {"code": {"$id": "#root/data/enteredBy/till/code",
                                                                    "type": ["string", "null"]},
                                                           "name": {"$id": "#root/data/enteredBy/till/name",
                                                                    "type": ["string", "null"]}}},
                                   "store": {"$id": "#root/data/enteredBy/store", "type": ["object", "null"],
                                             "properties": {"code": {"$id": "#root/data/enteredBy/store/code",
                                                                     "type": ["string", "null"]},
                                                            "name": {"$id": "#root/data/enteredBy/store/name",
                                                                     "type": ["string", "null"]}, "externalId": {
                                                     "$id": "#root/data/enteredBy/store/externalId",
                                                     "type": ["string", "null"]}, "externalId1": {
                                                     "$id": "#root/data/enteredBy/store/externalId1",
                                                     "type": ["string", "null"]}, "externalId2": {
                                                     "$id": "#root/data/enteredBy/store/externalId2",
                                                     "type": ["string", "null"]}}}}},
                               "customerIdentifiers": {"$id": "#root/data/customerIdentifiers",
                                                       "type": ["object", "null"], "properties": {
                                       "customerId": {"$id": "#root/data/customerIdentifiers/customerId",
                                                      "type": ["integer", "null"]},
                                       "loyaltyType": {"$id": "#root/data/customerIdentifiers/loyaltyType",
                                                       "type": ["string", "null"]},
                                       "instore": {"$id": "#root/data/customerIdentifiers/instore",
                                                   "type": ["object", "null"], "properties": {
                                               "mobile": {"$id": "#root/data/customerIdentifiers/instore/mobile",
                                                          "type": ["string", "null"]}, "externalId": {
                                                   "$id": "#root/data/customerIdentifiers/instore/externalId",
                                                   "type": ["string", "null"]}}}}},
                               "customFields": {"$id": "#root/data/customFields", "type": ["array", "null"],
                                                "items": {"$id": "#root/data/customFields/items",
                                                          "type": ["object", "null"], "properties": {
                                                        "key": {"$id": "#root/data/customFields/items/key",
                                                                "type": ["string", "null"]},
                                                        "value": {"$id": "#root/data/customFields/items/value",
                                                                  "type": ["string", "null"]}}}},
                               "extendedFields": {"$id": "#root/data/extendedFields", "type": ["array", "null"],
                                                  "items": {"$id": "#root/data/extendedFields/items",
                                                            "type": ["object", "null"], "properties": {
                                                          "key": {"$id": "#root/data/extendedFields/items/key",
                                                                  "type": ["string", "null"]},
                                                          "value": {"$id": "#root/data/extendedFields/items/value",
                                                                    "type": ["string", "null"]}}}}}},
                           "loyaltyEventId": {"$id": "#root/loyaltyEventId", "type": ["string", "null"]}}})

# STAFF PERFORMANCE
rop_employee_task = JsonSchemaValidator(schema={
    "properties": {
        "TaskId": {"type": ["string", "null"]},
        "FromDate": {"type": ["string", "null"]},
        "ToDate": {"type": ["string", "null"]},
        "Store": {"type": ["string", "null"]},
        "TodoId": {"type": ["number", "null"]},
        "TodoDetailId": {"type": ["number", "null"]},
        "TicketId": {"type": ["number", "null"]},
        "TicketNote": {"type": ["string", "null"]},
        "CompletedDate": {"type": ["string", "null"]},
        "CompletedBy": {"type": ["number", "null"]},
        "CompletedByName": {"type": ["string", "null"]},
        "EmployeeCode": {"type": ["string", "null"]}
    },
    "required": [
        "TaskId",
        "FromDate",
        "ToDate",
        "Store",
        "TodoId",
        "TodoDetailId",
        "TicketId",
        "TicketNote",
        "CompletedDate",
        "CompletedBy",
        "CompletedByName",
        "EmployeeCode"
    ],
    "title": "Schema for F_EMPLOYEE_TASK",
    "type": "object",
    "additionalProperties": True,
})

rop_store_checklist = JsonSchemaValidator(schema={
    "definitions":
        {
            "GroupDetails":
                {
                    "properties":
                        {
                            "TodoId": {"type": ["number", "null"]},
                            "TodoName": {"type": ["string", "null"]},
                            "IsActive": {"type": ["number", "null"]},
                            "Time": {"type": ["string", "null"]},
                            "TypeId": {"type": ["number", "null"]},
                            "RoleId": {"type": ["number", "null"]},
                            "GroupId": {"type": ["number", "null"]},
                            "GroupName": {"type": ["string", "null"]},
                            "VideoURL": {"type": ["string", "null"]},
                            "TodoDetailId": {"type": ["number", "null"]},
                            "SortOrder": {"type": ["number", "null"]},
                            "DetailDesc": {"type": ["string", "null"]},
                            "TimeDesc": {"type": ["string", "null"]},
                            "TimeFrom": {"type": ["string", "null"]},
                            "TimeTo": {"type": ["string", "null"]},
                            "IsRequiredPhoto": {"type": ["boolean", "null"]},
                            "Title": {"type": ["string", "null"]},
                            "ZoneId": {"type": ["string", "null"]},
                            "NodeTypeId": {"type": ["number", "null"]},
                            "NodeTypeName": {"type": ["string", "null"]},
                            "SyncingDate": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                },
            "NodeTypes":
                {
                    "properties":
                        {
                            "TodoId": {"type": ["number", "null"]},
                            "NodeTypeId": {"type": ["number", "null"]},
                            "NodeTypeName": {"type": ["string", "null"]}
                        },
                    "additionalProperties": False
                }
        },
    "type": "object",
    "required": ["GroupDetails", "NodeTypes"],
    "additionalProperties": False,
    "properties":
        {
            "TodoId": {"type": ["number", "null"]},
            "TodoName": {"type": ["string", "null"]},
            "IsActive": {"type": ["number", "null"]},
            "Time": {"type": ["string", "null"]},
            "TypeId": {"type": ["number", "null"]},
            "RoleId": {"type": ["number", "null"]},
            "SyncingDate": {"type": ["string", "null"]},
            "GroupDetails": {"type": "array", "items": {"$ref": "#/definitions/GroupDetails"}},
            "NodeTypes": {"type": "array", "items": {"$ref": "#/definitions/NodeTypes"}},
            "ProcDate": {"type": ["string", "null"]} #Added by CT
        }
})

rop_employee_checkinout = JsonSchemaValidator(schema={
    "properties": {
        "UserId": {"type": ["number", "null"]},
        "UserName": {"type": ["string", "null"]},
        "EmployeeCode": {"type": ["string", "null"]},
        "TnId": {"type": ["number", "null"]},
        "SiteId": {"type": ["string", "null"]},
        "SiteName": {"type": ["string", "null"]},
        "CheckInTime": {"type": ["string", "null"]},
        "Lat": {"type": ["number", "null"]},
        "Lon": {"type": ["number", "null"]},
        "Distance": {"type": ["number", "null"]},
        "ClientIP": {"type": ["string", "null"]},
        "ClientIMEI": {"type": ["string", "null"]},
        "SiteIPRange": {"type": ["string", "null"]},
        "SiteLongitude": {"type": ["number", "null"]},
        "SiteLatitude": {"type": ["number", "null"]},
        "LocationAddress": {"type": ["string", "null"]},
        "CheckingType": {"type": ["number", "null"]}
    },
    "required": [
        "UserId",
        "UserName",
        "EmployeeCode",
        "TnId",
        "SiteId",
        "SiteName",
        "CheckInTime",
        "Lat",
        "Lon",
        "Distance",
        "ClientIP",
        "ClientIMEI",
        "SiteIPRange",
        "SiteLongitude",
        "SiteLatitude",
        "LocationAddress",
        "CheckingType"
    ],
    "title": "Schema for F_EMPLOYEE_CHECKINOUT",
    "type": "object",
    "additionalProperties": True,
})