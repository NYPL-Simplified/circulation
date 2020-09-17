EXISTING_BOOK_FILE_PATH = '/books/ebook.epub'
NOT_EXISTING_BOOK_FILE_PATH = '/books/notexistingbook.epub'

BOOK_IDENTIFIER = 'EBOOK'

CONTENT_ENCRYPTION_KEY = '+RulyN2G8MfAahNEO/Xz0TwBT5xMzvbFFHqqWGPrO3M='
PROTECTED_CONTENT_LOCATION = '/opt/readium/files/encrypted/1f162bc2-be6f-42a9-8153-96d675418ff1.epub'
PROTECTED_CONTENT_DISPOSITION = '1f162bc2-be6f-42a9-8153-96d675418ff1.epub'
PROTECTED_CONTENT_TYPE = 'application/epub+zip'
PROTECTED_CONTENT_LENGTH = 798385
PROTECTED_CONTENT_SHA256 = 'e058281cbc11bae29451e5e2c8003efa1164c3f6dde6dcc003c8bb79e2acb88f'


LCPENCRYPT_NOT_EXISTING_DIRECTORY_RESULT = \
    '''Error opening input file, for more information type 'lcpencrypt -help' ; level 30
open {0}: no such file or directory
'''.format(NOT_EXISTING_BOOK_FILE_PATH)

LCPENCRYPT_FAILED_ENCRYPTION_RESULT = \
    '''{{
   "content-id": "{0}",
   "content-encryption-key": null,
   "protected-content-location": "{1}",
   "protected-content-length": null,
   "protected-content-sha256": null,
   "protected-content-disposition": "{2}"
}}
Encryption was successful
'''.format(
        BOOK_IDENTIFIER,
        PROTECTED_CONTENT_LOCATION,
        NOT_EXISTING_BOOK_FILE_PATH
    )

LCPENCRYPT_SUCCESSFUL_ENCRYPTION_RESULT = \
    '''{{
   "content-id": "{0}",
   "content-encryption-key": "{1}",
   "protected-content-location": "{2}",
   "protected-content-length": {3},
   "protected-content-sha256": "{4}",
   "protected-content-disposition": "{5}",
   "protected-content-type": "{6}"
}}
Encryption was successful
'''.format(
        BOOK_IDENTIFIER,
        CONTENT_ENCRYPTION_KEY,
        PROTECTED_CONTENT_LOCATION,
        PROTECTED_CONTENT_LENGTH,
        PROTECTED_CONTENT_SHA256,
        PROTECTED_CONTENT_DISPOSITION,
        PROTECTED_CONTENT_TYPE
    )

LCPENCRYPT_FAILED_LCPSERVER_NOTIFICATION = \
    '''Error notifying the License Server; level 60
lcp server error 401'''

LCPENCRYPT_SUCCESSFUL_NOTIFICATION_RESULT = \
    '''License Server was notified
{{
   "content-id": "{0}",
   "content-encryption-key": "{1}",
   "protected-content-location": "{2}",
   "protected-content-length": {3},
   "protected-content-sha256": "{4}",
   "protected-content-disposition": "{5}",
   "protected-content-type": "{6}"
}}
Encryption was successful
'''.format(
        BOOK_IDENTIFIER,
        CONTENT_ENCRYPTION_KEY,
        PROTECTED_CONTENT_LOCATION,
        PROTECTED_CONTENT_LENGTH,
        PROTECTED_CONTENT_SHA256,
        PROTECTED_CONTENT_DISPOSITION,
        PROTECTED_CONTENT_TYPE
    )


LCPSERVER_LICENSE = '''
{
  "provider": "http://circulation.manager",
  "id": "e99be177-4902-426a-9b96-0872ae877e2f",
  "issued": "2020-08-18T15:04:39Z",
  "encryption": {
    "profile": "http://readium.org/lcp/basic-profile",
    "content_key": {
      "algorithm": "http://www.w3.org/2001/04/xmlenc#aes256-cbc",
      "encrypted_value": "rYjD9ijFELcraQvdeChvvI21ceHwF3XXN6e4tQpoCbDnnekb9UeGZVlocqANwJ28S0QnJPQk0EnDD6KEIS4dzw=="
    },
    "user_key": {
      "algorithm": "http://www.w3.org/2001/04/xmlenc#sha256",
      "text_hint": "Not very helpful hint",
      "key_check": "zf2gU5H8+JIYVbJB2AyotuAq+Fc6xQo85bkhqtWqIU4EVzewwv6HdHgUXvRZB+zp1yZdCTlQvbhA4SQv5oydCQ=="
    }
  },
  "links": [{
    "rel": "hint",
    "href": "http://testfrontend:8991/static/hint.html"
  }, {
    "rel": "publication",
    "href": "http://localhost:9000/books/9780231543973",
    "type": "application/pdf+lcp",
    "title": "9780231543973.lcpdf",
    "length": 1703749,
    "hash": "6657273fe78fb29472a0027c08254f57e58b61fe435c30978c00aacd55247bfd"
  }, {
    "rel": "status",
    "href": "http://lsdserver:8990/licenses/e99be177-4902-426a-9b96-0872ae877e2f/status",
    "type": "application/vnd.readium.license.status.v1.0+json"
  }],
  "user": {
    "id": "1"
  },
  "rights": {
    "print": 10,
    "copy": 2048,
    "start": "2020-08-18T15:04:38Z",
    "end": "2020-09-08T15:04:38Z"
  },
  "signature": {
    "certificate": "MIIFpTCCA42gAwIBAgIBATANBgkqhkiG9w0BAQsFADBnMQswCQYDVQQGEwJGUjEOMAwGA1UEBxMFUGFyaXMxDzANBgNVBAoTBkVEUkxhYjESMBAGA1UECxMJTENQIFRlc3RzMSMwIQYDVQQDExpFRFJMYWIgUmVhZGl1bSBMQ1AgdGVzdCBDQTAeFw0xNjAzMjUwMzM3MDBaFw0yNjAzMjMwNzM3MDBaMIGQMQswCQYDVQQGEwJGUjEOMAwGA1UEBxMFUGFyaXMxDzANBgNVBAoTBkVEUkxhYjESMBAGA1UECxMJTENQIFRlc3RzMSIwIAYDVQQDExlUZXN0IHByb3ZpZGVyIGNlcnRpZmljYXRlMSgwJgYJKoZIhvcNAQkBFhlsYXVyZW50LmxlbWV1ckBlZHJsYWIub3JnMIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAq/gFXdvKb+EOzsEkHcoSOcPQmNzivzf+9NOJcxWi1/BwuxqAAPv+4LKoLz89U1xx5TE1swL11BsEkIdVYrjl1RiYRa8YV4bb4xyMTm8lm39P16H1fG7Ep8yyoVuN6LT3WT2xHGp2jYU8I2nW78cyYApAWAuiMc3epeIOxC2mKgf1pGnaX9j5l/Rx8hhxULqoHIHpR8e1eVRC7tgAz4Oy5qeLxGoL4S+GK/11eRlDO37whAWaMRbPnJDqqi8Z0Beovf6jmdoUTJdcPZZ9kFdtPsWjPNNHDldPuJBtCd7lupc0K4pClJSqtJKyxs05Yeb1j7kbs/i3grdlUcxz0zOaPN1YzrzOO7GLEWUnIe+LwVXAeUseHedOexITyDQXXCqMoQw/BC6ApGzR0FynC6ojq98tStYGJAGbKBN/9p20CvYf4/hmPU3fFkImWguPIoeJT//0rz+nSynykeEVtORRIcdyOnX2rL03xxBW7qlTlUXOfQk5oLIWXBW9Z2Q63MPWi8jQhSI0jC12iEqCT54xKRHNWKr04at9pJL85M0bDCbBH/jJ+AIbVx02ewtXcWgWTgK9vgSPN5kRCwIGaV9PMS193KHfNpGqV45EKrfP8U2nvNDeyqLqAN5847ABSW7UmA5Kj/x5uGxIWu9MUKjZlT0FpepswFvMMo1InLHANMcCAwEAAaMyMDAwDAYDVR0TAQH/BAIwADALBgNVHQ8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZIhvcNAQELBQADggIBAEGAqzHsCbrfQwlWas3q66FG/xbiOYQxpngA4CZWKQzJJDyOFgWEihW+H6NlSIH8076srpIZByjEGXZfOku4NH4DGNOj6jQ9mEfEwbrvCoEVHQf5YXladXpKqZgEB2FKeJVjC7yplelBtjBpSo23zhG/o3/Bj7zRySL6gUCewn7z/DkxM6AshDE4HKQxjxp7stpESev+0VTL813WXvwzmucr94H1VPrasFyVzQHj4Ib+Id1OAmgfzst0vSZyX6bjAuiN9yrs7wze5cAYTaswWr7GAnAZ/r1Z3PiDp50qaGRhHqJ+lRAhihpFP+ZjsYWRqnxZnDzJkJ6RZAHi2a3VN8x5WhOUMTf3JZcFVheDmA4SaEjAZAHU8zUxx1Fstjc8GJcjTwWxCsVM2aREBKXAYDhPTVLRKt6PyQxB0GxjDZZSvGI9uXn6S5wvjuE4T2TUwbJeGHqJr4FNpXVQ2XNww+sV2QSiAwrlORm8HNXqavj4rqz1PkUySXJ6b7zbjZoiACq4C7zb70tRYDyCfLTYtaTL3UK2Sa9ePSl0Fe6QfcqlGjalrqOo4GI6oqbAIkIXocHHksbLx0mIMSEWQOax+DqXhsl8tNGVwa5EiUSy83Sc0LyYXoWA35q8dugbkeNnY94rNG/hYKeci1VHhyg4rqxEeVwfBx121JqQSs+hHGKt",
    "value": "pbfPRtb4oDT+1Q8nVrZuFrP/uCFqDG+/+jC3pUJfp+iLU+cBVWNCmciADVuq25UkpNOdiTAre8Xjglz1WVV+2AZjiLEaKjQZN0kjYLFjxSC67vUcHc6g5KpAQQTHSbjed5LAjShJWeVkIGQxQFP1a1o+cky8y1tzzRWoZZjCQHTj2ob621cAYgw39z2mj+oKm/vPIYbCrIlahSvjBMCOkWTOoRNZIuqnapRUv25OB9JQeqJzvotTOQvoxZpFg5q3EEmkZAIW55u6XBRaP9CvIAlDuCzevOVT1CojeyVPlP2nWs8b9oBp77S/SYEK0ZYMWMQ0S4LnAB8CNHdGEmF4+jvhrAAOgwpsiMRH0eMQAGZnzPUSKYIr/RqSd7Mp53nFn4a18dGcBgRxipCnVPafU+B7HwWcvkYBu4idlN3tFH1fjPl18yz0qHa8+RlTIyyw73CGQ8SUAY87BLO8tmBKihP+FePqnPX1Fbp6MprI6K4/GkWZoOe3n1oauVLIe7T0CRsA5rar2loUlIJsfESDj5tFnSh4UOeHA0ewHrzDS2qtdFL7sREZ/CnlDJPr0wuZB+uAyECrWe5FuQpEiSP2vxi9ROvTeZuUhphVghFPvBwunlL3AB/6GXkbnlKSJUAb3wiRNWk3r0ilVu9ORSsdq00IzShHGyy8DMVP+5dXSU4=",
    "algorithm": "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
  }
}
'''

LCPSERVER_URL = 'http://localhost:8989'
LCPSERVER_USER = 'lcp'
LCPSERVER_PASSWORD = 'secretpassword'
LCPSERVER_INPUT_DIRECTORY = '/opt/readium/encrypted'

CONTENT_ID = '1'
TEXT_HINT = 'Not very helpful hint'
PROVIDER_NAME = 'http://circulation.manager'
