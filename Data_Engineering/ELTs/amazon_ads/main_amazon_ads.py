import os,sys
scriptPath = os.path.realpath(os.path.dirname(sys.argv[0]))
superPath = os.path.realpath(os.path.dirname(scriptPath))
sys.path.append(superPath)
from etl_amazon_ads import AmazonAdsETL
import json 
import traceback
import time 

sponsoredProduct = {'reportTypeId': 'spCampaigns',
                    'adProduct': 'SPONSORED_PRODUCTS',
                    'fields':["campaignName","campaignId","campaignStatus","impressions", "clicks", "cost", "purchases1d", "purchases7d", "purchases14d", "purchases30d", "purchasesSameSku1d", "purchasesSameSku7d", "purchasesSameSku14d", "purchasesSameSku30d", "unitsSoldClicks1d", "unitsSoldClicks7d", "unitsSoldClicks14d", "unitsSoldClicks30d", "sales1d", "sales7d", "sales14d", "sales30d", "attributedSalesSameSku1d", "attributedSalesSameSku7d", "attributedSalesSameSku14d", "attributedSalesSameSku30d", "unitsSoldSameSku1d", "unitsSoldSameSku7d", "unitsSoldSameSku14d", "unitsSoldSameSku30d", "kindleEditionNormalizedPagesRead14d", "kindleEditionNormalizedPagesRoyalties14d", "date", "campaignBiddingStrategy", "costPerClick", "clickThroughRate", "spend"]}
sponsoredDisplay = {'reportTypeId': 'sdCampaigns',
                    'adProduct': 'SPONSORED_DISPLAY',
                    'fields': ["addToCart", "addToCartClicks", "addToCartRate", "addToCartViews", "brandedSearches", "brandedSearchesClicks", "brandedSearchesViews", "brandedSearchRate", "campaignBudgetCurrencyCode", "campaignId", "campaignName","campaignStatus", "clicks", "cost", "date", "detailPageViews", "detailPageViewsClicks", "eCPAddToCart", "eCPBrandSearch",  "impressions", "impressionsViews", "newToBrandPurchases", "newToBrandPurchasesClicks", "newToBrandSalesClicks", "newToBrandUnitsSold", "newToBrandUnitsSoldClicks", "purchases", "purchasesClicks", "purchasesPromotedClicks", "sales", "salesClicks", "salesPromotedClicks", "unitsSold", "unitsSoldClicks", "videoCompleteViews", "videoFirstQuartileViews", "videoMidpointViews", "videoThirdQuartileViews", "videoUnmutes", "viewabilityRate", "viewClickThroughRate"]}
sponsoredBrand = {'reportTypeId': 'sbCampaigns',
                  'adProduct': 'SPONSORED_BRANDS',
                  'fields': ["addToCart", "addToCartClicks", "addToCartRate", "brandedSearches", "brandedSearchesClicks", "campaignBudgetAmount", "campaignBudgetCurrencyCode", "campaignBudgetType", "campaignId", "campaignName", "campaignStatus", "clicks", "cost", "costType", "date", "detailPageViews", "detailPageViewsClicks", "eCPAddToCart",  "impressions", "newToBrandDetailPageViewRate", "newToBrandDetailPageViews", "newToBrandDetailPageViewsClicks", "newToBrandECPDetailPageView", "newToBrandPurchases", "newToBrandPurchasesClicks", "newToBrandPurchasesPercentage", "newToBrandPurchasesRate", "newToBrandSales", "newToBrandSalesClicks", "newToBrandSalesPercentage", "newToBrandUnitsSold", "newToBrandUnitsSoldClicks", "newToBrandUnitsSoldPercentage", "purchases", "purchasesClicks", "purchasesPromoted", "sales", "salesClicks", "salesPromoted",  "topOfSearchImpressionShare", "unitsSold", "unitsSoldClicks", "video5SecondViewRate", "video5SecondViews", "videoCompleteViews", "videoFirstQuartileViews", "videoMidpointViews", "videoThirdQuartileViews", "videoUnmutes", "viewabilityRate", "viewableImpressions", "viewClickThroughRate"]}
sponsoredTelevisions = {'reportTypeId': 'stCampaigns',
                        'adProduct': 'SPONSORED_TELEVISIONS',
                        'fields':["addToCart", "addToCartClicks", "addToCartViews", "brandedSearches", "brandedSearchesClicks", "brandedSearchesViews", "clicks", "clickThroughRate", "cost", "costPerThousandImpressions", "date", "detailPageViews", "detailPageViewsClicks", "detailPageViewsViews",  "impressions", "newToBrandDetailPageViewClicks", "newToBrandDetailPageViews", "newToBrandDetailPageViewViews", "newToBrandPurchases", "newToBrandPurchasesClicks", "newToBrandPurchasesViews", "newToBrandSales", "newToBrandSalesClicks", "newToBrandSalesViews", "portfolioId", "purchases", "purchasesClicks", "purchasesViews", "roas", "sales", "salesClicks", "salesViews", "unitsSold", "unitsSoldClicks", "unitsSoldViews", "videoCompleteViews", "videoFirstQuartileViews", "videoMidpointViews", "videoThirdQuartileViews"]}

reportQueries = [sponsoredProduct,sponsoredDisplay,sponsoredBrand]

def main(a='a',b='b'):
    project_id = '{{project_id}}'
    storageServiceAccountCredential = json.load(open(f'./src/var/login_credentials/{project_id}/storage_service_account.json'))
    amazonAdsETL = AmazonAdsETL(storageServiceAccountCredential=storageServiceAccountCredential,debug=False)
    try:
        print('Initiating new reports...')
        new_reports = amazonAdsETL.initiate_reports(reportQueries)
    except Exception as e:
        print('Error initiating reports')
        traceback.print_exc()
        new_reports = {}
    try:
        print(f"{len(new_reports)} reports initiated")
        amazonAdsETL.reports.update(new_reports)
        print(f"{len(amazonAdsETL.reports)} reports in total")
        time.sleep(30)
        print(f"Starting fetching reports...")
        amazonAdsETL.get_reports(amazonAdsETL.reports)
    except Exception as e:
        print('Error fetching reports')
        traceback.print_exc()