from dotenv import load_dotenv
import os
import logging
load_dotenv(override=True)

class Config:
    PROGRAM_ID = 1
    DOCUMENT_FORMAT = 'PDF'

    COUNTRIES = {
        'Nigeria': {
            'starting_docket_id': 1001,
            'url': 'https://nafdac.gov.ng/vaccines-biologicals/vaccines-biologicals-guidelines/',
            'agency_id': 'NAFDAC',
            'save_dir': r'C:\Users\HP\AppData\Local\Temp',
            'timeout': 10,
            'country': 'Nigeria'
        },
        'south_africa': { 
            'starting_docket_id': 2001,
            'url': 'https://www.sahpra.org.za/medical-devices-and-in-vitro-diagnostics-guidelines/',
            'agency_id': 'SAHPRA',
            'save_dir': r'C:\Users\HP\AppData\Local\Temp',
            'timeout': 10,
            'country': 'South Africa' 
        },
        'Singapore': {
            'url': 'https://www.hsa.gov.sg/medical-devices/guidance-documents',
            'country': 'Singapore',
            'starting_docket_id': 3001,
            'agency_id': 'HSA'
        },
        'Thailand': {
            'url': 'https://en.fda.moph.go.th/cat2-health-products/category/health-products-medical-devices?ppp=20&page=1',
            'country': 'Thailand',
            'starting_docket_id': 4001,
            'agency_id': 'TH-FDA'
        },
        'Ireland': {
            'url': 'https://www.hpra.ie/regulation/medical-devices/documents-and-guidance/guidance-documents',
            'country': 'Ireland',
            'starting_docket_id': 5001,
            'agency_id': 'IE-HPRA'
        },
        'Canada': {
            'url': 'https://www.canada.ca/en/health-canada/services/drugs-health-products/medical-devices/application-information/guidance-documents.html',
            'country': 'Canada',
            'starting_docket_id': 6001,
            'agency_id': 'CA-HEALTH'
        },
        'belgium': {
            'url': 'https://www.afmps.be/fr/usage_humain/produits_de_sante/dispositifs_medicaux_et_leurs_accessoires/etablissements_et_5',
            'country': 'Belgium',
            'starting_docket_id': 7001,
            'agency_id': 'AFMPS'
        }
    }

    @classmethod
    def get_country_config(cls, country):
        if not country:
            return {}
        # Case-insensitive lookup
        country_lower = country.lower()
        for key in cls.COUNTRIES:
            if key.lower() == country_lower:
                return cls.COUNTRIES[key]
        logging.error(f"No configuration found for country: {country}")
        return {}

# from dotenv import load_dotenv
# import os

# load_dotenv(override=True)

# class Config:
#     PROGRAM_ID = 1
#     DOCUMENT_FORMAT = 'PDF'

#     COUNTRIES = {
#         'Nigeria': {
#             'starting_docket_id': 1001,
#             'url': 'https://nafdac.gov.ng/vaccines-biologicals/vaccines-biologicals-guidelines/', 
#             'agency_id': 'NAFDAC',
#              'save_dir': r'C:\Users\HP\AppData\Local\Temp',
#                 'timeout': 10,
#             'country': 'Nigeria'
#         },
#        'south_africa': {
#             'starting_docket_id': 2001,
#             'url': 'https://www.sahpra.org.za/medical-devices-and-in-vitro-diagnostics-guidelines/', 
#             'agency_id': 'SAHPRA',
#              'save_dir': r'C:\Users\HP\AppData\Local\Temp',
#                 'timeout': 10,
#             'country': 'south_africa'
#         },
#        'Singapore': {
#                 'url': 'https://www.hsa.gov.sg/medical-devices/guidance-documents',  # Replace with actual guidelines page
#                 'country': 'Singapore',
#                 'starting_docket_id': 3000,
#                 'agency_id': 'HSA'
#             },
#        'Thailand': {
#                 'url': 'https://en.fda.moph.go.th/cat2-health-products/category/health-products-medical-devices?ppp=20&page=1',
#                 'country': 'Thailand',
#                 'starting_docket_id': 4000,
#                 'agency_id': 'TH-FDA'
#             },
#             'Ireland': {
#                 'url': 'https://www.hpra.ie',  # Update with specific page if needed
#                 'country': 'Ireland',
#                 'starting_docket_id': 5000,
#                 'agency_id': 'IE-HPRA'
#             },
#             'Canada': {
#                 'url': 'https://www.canada.ca',  # Update with specific page
#                 'country': 'Canada',
#                 'starting_docket_id': 6000,
#                 'agency_id': 'CA-HEALTH'
#             }
#     }

#     @classmethod
#     def get_country_config(cls, country):
#         return cls.COUNTRIES.get(country, {})


# # Config.py
# from dotenv import load_dotenv
# import os

# load_dotenv(override=True)

# class Config:
#     # AWS Credentials from .env (for later use)
#     AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
#     AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
#     AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
#     S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

#     # Static values
#     PROGRAM_ID = 1
#     DOCUMENT_FORMAT = 'PDF'  # Uppercase as specified

#     # S3 folder structure template (for later)
#     S3_FOLDER_STRUCTURE = 'INTERNATIONAL_DOCS/{country}/{docket_id}/{doc_id}.{ext}'

#     # Country-specific configurations
#     COUNTRIES = {
#         'Nigeria': {
#             'starting_docket_id': 1001,
#             'url': 'https://nafdac.gov.ng/vaccines-biologicals/vaccines-biologicals-guidelines/',  # Example URL; replace with actual if different
#             'agency_id': 'NCDC',  # Assumed agency
#             # Add other fixed details if needed
#         },
#         # Add more countries later
#     }

#     @classmethod
#     def get_country_config(cls, country):
#         return cls.COUNTRIES.get(country, {})