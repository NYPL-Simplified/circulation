import datetime
import re

from onelogin.saml2.utils import OneLogin_Saml2_Utils

from api.saml.metadata.model import SAMLBinding, SAMLNameIDFormat

NAME_ID_FORMAT_1 = "urn:mace:shibboleth:1.0:nameIdentifier"
NAME_ID_FORMAT_2 = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"

IDP_1_ENTITY_ID = "http://idp1.hilbertteam.net/idp/shibboleth"
IDP_1_UI_INFO_DISPLAY_NAME = "Shibboleth Test IdP 1"
IDP_1_UI_INFO_EN_DISPLAY_NAME = IDP_1_UI_INFO_DISPLAY_NAME
IDP_1_UI_INFO_ES_DISPLAY_NAME = IDP_1_UI_INFO_DISPLAY_NAME
IDP_1_UI_INFO_DESCRIPTION = "Shibboleth Test IdP 1"
IDP_1_UI_INFO_INFORMATION_URL = "http://idp1.hilbertteam.net"
IDP_1_UI_INFO_PRIVACY_STATEMENT_URL = "http://idp1.hilbertteam.net"
IDP_1_UI_INFO_LOGO_URL = "http://idp1.hilbertteam.net/logo.png"

IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME = IDP_1_UI_INFO_DISPLAY_NAME
IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME = IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME
IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME = IDP_1_UI_INFO_DISPLAY_NAME
IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME = (
    IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME
)
IDP_1_ORGANIZATION_EN_ORGANIZATION_URL = IDP_1_UI_INFO_INFORMATION_URL
IDP_1_ORGANIZATION_ES_ORGANIZATION_URL = IDP_1_ORGANIZATION_EN_ORGANIZATION_URL

IDP_1_SSO_URL = "http://idp1.hilbertteam.net/idp/profile/SAML2/Redirect/SSO"
IDP_1_SSO_BINDING = SAMLBinding.HTTP_REDIRECT

IDP_2_ENTITY_ID = "http://idp2.hilbertteam.net/idp/shibboleth"
IDP_2_UI_INFO_DISPLAY_NAME = "Shibboleth Test IdP 2"
IDP_2_UI_INFO_EN_DISPLAY_NAME = IDP_2_UI_INFO_DISPLAY_NAME
IDP_2_UI_INFO_ES_DISPLAY_NAME = IDP_2_UI_INFO_DISPLAY_NAME
IDP_2_UI_INFO_DESCRIPTION = "Shibboleth Test IdP 2"
IDP_2_UI_INFO_INFORMATION_URL = "http://idp2.hilbertteam.net"
IDP_2_UI_INFO_PRIVACY_STATEMENT_URL = "http://idp2.hilbertteam.net"
IDP_2_UI_INFO_LOGO_URL = "http://idp2.hilbertteam.net/logo.png"

IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME = IDP_2_UI_INFO_DISPLAY_NAME
IDP_2_ORGANIZATION_ES_ORGANIZATION_NAME = IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME
IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME = IDP_2_UI_INFO_DISPLAY_NAME
IDP_2_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME = (
    IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME
)
IDP_2_ORGANIZATION_EN_ORGANIZATION_URL = IDP_2_UI_INFO_INFORMATION_URL
IDP_2_ORGANIZATION_ES_ORGANIZATION_URL = IDP_2_ORGANIZATION_EN_ORGANIZATION_URL

IDP_2_SSO_URL = "http://idp2.hilbertteam.net/idp/profile/SAML2/Redirect/SSO"
IDP_2_SSO_BINDING = SAMLBinding.HTTP_REDIRECT

SP_ENTITY_ID = "http://sp.hilbertteam.net/idp/shibboleth"
SP_UI_INFO_DISPLAY_NAME = "Shibboleth Test SP"
SP_UI_INFO_EN_DISPLAY_NAME = SP_UI_INFO_DISPLAY_NAME
SP_UI_INFO_ES_DISPLAY_NAME = SP_UI_INFO_DISPLAY_NAME
SP_UI_INFO_DESCRIPTION = "Shibboleth Test SP"
SP_UI_INFO_INFORMATION_URL = "http://sp.hilbertteam.net"
SP_UI_INFO_PRIVACY_STATEMENT_URL = "http://sp.hilbertteam.net"
SP_UI_INFO_LOGO_URL = "http://sp.hilbertteam.net/logo.png"

SP_ORGANIZATION_EN_ORGANIZATION_NAME = SP_UI_INFO_DISPLAY_NAME
SP_ORGANIZATION_ES_ORGANIZATION_NAME = SP_ORGANIZATION_EN_ORGANIZATION_NAME
SP_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME = SP_UI_INFO_DISPLAY_NAME
SP_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME = (
    SP_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME
)
SP_ORGANIZATION_EN_ORGANIZATION_URL = SP_UI_INFO_INFORMATION_URL
SP_ORGANIZATION_ES_ORGANIZATION_URL = SP_ORGANIZATION_EN_ORGANIZATION_URL

SP_ACS_URL = "http://sp.hilbertteam.net/idp/profile/SAML2/POST"
SP_ACS_BINDING = SAMLBinding.HTTP_POST

SIGNING_CERTIFICATE = """MIICXDCCAcWgAwIBAgIBADANBgkqhkiG9w0BAQ0FADBLMQswCQYDVQQGEwJ1czEN
MAsGA1UECAwET2hpbzETMBEGA1UECgwKQUNNRSwgSW5jLjEYMBYGA1UEAwwPaGls
YmVydHRlYW0ubmV0MB4XDTIwMDUxODE4MjUyM1oXDTIxMDUxODE4MjUyM1owSzEL
MAkGA1UEBhMCdXMxDTALBgNVBAgMBE9oaW8xEzARBgNVBAoMCkFDTUUsIEluYy4x
GDAWBgNVBAMMD2hpbGJlcnR0ZWFtLm5ldDCBnzANBgkqhkiG9w0BAQEFAAOBjQAw
gYkCgYEAqx63LDc2vjoYlTvddjPOKDsduR0++A1lmGxdX1N6Ei4NRiWaqBnVij6m
Oqzq5quMA8M3du71aVzE0ELJOuhzrjpu6Rn40KGO6Ewiv3StQkbwAArrrIlIPA9U
MpsGD+/oNXlbF9ZbfqvxEoZcEk6XR6fJT7zXBNzp75dCi39D53MCAwEAAaNQME4w
HQYDVR0OBBYEFCmRcv2NFCSso9IRSFQsdST5FpBfMB8GA1UdIwQYMBaAFCmRcv2N
FCSso9IRSFQsdST5FpBfMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQENBQADgYEA
c/ddQRAswvrlYD8IOA9TCjyqkUJmyJBOj+d0PTzW7lF7NUyPSp0SunDq12RD8imV
q15wNzuzsiIfUZ7F/sp1iFH8ASrBS4sk39stDgUcjFNcwekihUGw3GfhGcniFvvi
a/F82fbPXBPajb9nXNyn3ZwlLsooeC06oIj8FlyHoR8=
"""

ENCRYPTION_CERTIFICATE = """MIIDEzCCAfugAwIBAgIUG6Nn1rlERS1vsi88tcdzSYX0oqAwDQYJKoZIhvcNAQEL
BQAwFTETMBEGA1UEAwwKaWRwdGVzdGJlZDAeFw0xNTEyMTEwMjIwMTRaFw0zNTEy
MTEwMjIwMTRaMBUxEzARBgNVBAMMCmlkcHRlc3RiZWQwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQCBXv0o3fmT8iluyLjJ4lBAVCW+ZRVyEXPYQuRi7vfD
cO4a6d1kxiJLsaK0W88VNxjFQRr8PgDkWr28vwoH1rgk4pLsszLD48DBzD942peJ
l/S6FnsIJjmaHcBh4pbNhU4yowu63iKkvttrcZAEbpEro6Z8CziWEx8sywoaYEQG
ifPkr9ORV6Cn3txq+9gMBePG41GrtZrUGIu+xrndL0Shh4Pq0eq/9MAsVlIIXEa8
9WfH8J2kFcTOfoWtIc70b7TLZQsx4YnNcnrGLSUEcstFyPLX+Xtv5SNZF89OOIxX
VNjNvgE5DbJb9hMM4UAFqI+1bo9QqtxwThjc/sOvIxzNAgMBAAGjWzBZMB0GA1Ud
DgQWBBStTyogRPuAVG6q7yPyav1uvE+7pTA4BgNVHREEMTAvggppZHB0ZXN0YmVk
hiFodHRwczovL2lkcHRlc3RiZWQvaWRwL3NoaWJib2xldGgwDQYJKoZIhvcNAQEL
BQADggEBAFMfoOv+oISGjvamq7+Y4G7ep5vxlAPeK3RATYPYvAmyH946qZXh98ni
QXyuqZW5P5eEt86toY45IwDU5r09SKwHughEe99iiEkxh0mb2qo84qX9/qcg+kyN
jeLd/OSyolpUCEFNwOFcog7pj7Eer+6AHbwTn1Mjb5TBsKwtDMJsaxPvdj0u7M5r
xL/wHkFhn1rCo2QiojzjSlV3yLTh49iTyhE3cG+RxaNKDCxhp0jSSLX1BW/ZoPA8
+PMJEA+Q0QbyRD8aJOHN5O8jGxCa/ZzcOnYVL6AsEXoDiY3vAUYh1FUonOWw0m9H
p+tGUbGS2l873J5PrsbpeKEVR/IIoKo=
"""


PRIVATE_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQCrTFLt+Grv0WAL4CDUUa3LnIL3NDAoDHOtlT95q+vUWLFUEWZk
k8jTXyHYh+4bWxkdpU6L0zVeyIs0UAR0FYi8LGCggxv82Z1NGufiQFS0fO1X0d7o
+PUFnPrI05ubalDKWTaxAUW6y+Qv2F9hIOKBN5vefGRJAnu3NCjqhNH3tQIDAQAB
AoGBAJWpUo9dyriK2vqlMKmwT0MlFHu9GhHzhyHy0wmP/rSBZTVJGofnYr/iTyTq
5kr6VcBEDJM3zlpops7m1m1B3zrtj58BRDzixxtK9CYtXBipp8ARak7raJhUezb6
RV3iqYangGK+KKh5N3cGCMj8/a8FHkGAqikWBTM1+kM6ge2FAkEA+Yfndqx3pNBD
fZuRdM15cmvWpSPjiVIxHYLs+eotEqIVHxQRgU2fr9Yr/xIFqC/aYGxFB6o0iZkl
hFqNikkX1wJBAK+9M6w9BnFAjoMWb6DuwSf3pAigUcc250VcnY83D7qaodGYn20I
dk7N6huI/qW6+NesCkDDUWfM5uo62VaPC1MCQF3NFNmro541G+nP3TIHO6wjlaCm
iRZJ62SonuSjfyYN/9qa9KmiwHCdFhTgdXfv5StyB9EFzpIbG9tRHvvo1ikCQBui
wzd4uUSiBEc1BgTmxtVEKjV5EKMlTSUpNVXKMN0FaCEPwTJiPEiUNRZzaqghSPSR
h2M709dg74cyDe+AuyUCQQDajeLDmTzRs2uU1RNPrR2dWzd8UEAE/yhT/S5drrXY
kF7xgXphcsIlNUxJyp79q30fpwUCCwwTcfimCWBzRCAf
-----END RSA PRIVATE KEY-----
"""

INCORRECT_XML = ""

INCORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_SSO_SERVICE = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{8}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{9}</NameIDFormat>
        <NameIDFormat>{10}</NameIDFormat>
    </IDPSSODescriptor>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    IDP_1_UI_INFO_DESCRIPTION,
    IDP_1_UI_INFO_INFORMATION_URL,
    IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_1_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
)

INCORRECT_XML_WITH_ONE_IDP_METADATA_WITH_SSO_SERVICE_WITH_WRONG_BINDING = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{8}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{9}</NameIDFormat>
        <NameIDFormat>{10}</NameIDFormat>
        <SingleSignOnService 
            Binding="{11}" 
            Location="{12}"/>
    </IDPSSODescriptor>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    IDP_1_UI_INFO_DESCRIPTION,
    IDP_1_UI_INFO_INFORMATION_URL,
    IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_1_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    SAMLBinding.HTTP_ARTIFACT.value,
    IDP_1_SSO_URL,
)

CORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_DISPLAY_NAMES = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{1}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{2}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{3}</NameIDFormat>
        <NameIDFormat>{4}</NameIDFormat>
        <SingleSignOnService 
            Binding="{5}" 
            Location="{6}"/>
    </IDPSSODescriptor>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_1_SSO_BINDING.value,
    IDP_1_SSO_URL,
)

CORRECT_XML_WITH_IDP_1 = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10" xml:lang="en">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{8}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{9}</NameIDFormat>
        <NameIDFormat>{10}</NameIDFormat>
        <SingleSignOnService 
            Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
            Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
        <SingleSignOnService 
            Binding="{11}" 
            Location="{12}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{13}</OrganizationName>
      <OrganizationName xml:lang="es">{14}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{15}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{16}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{17}</OrganizationURL>
      <OrganizationURL xml:lang="es">{18}</OrganizationURL>
    </Organization>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    IDP_1_UI_INFO_DESCRIPTION,
    IDP_1_UI_INFO_INFORMATION_URL,
    IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_1_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_1_SSO_BINDING.value,
    IDP_1_SSO_URL,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_URL,
)

CORRECT_XML_WITH_IDP_2 = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{8}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{9}</NameIDFormat>
        <NameIDFormat>{10}</NameIDFormat>
        <SingleSignOnService 
            Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
            Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
        <SingleSignOnService 
            Binding="{11}" 
            Location="{12}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{13}</OrganizationName>
      <OrganizationName xml:lang="es">{14}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{15}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{16}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{17}</OrganizationURL>
      <OrganizationURL xml:lang="es">{18}</OrganizationURL>
    </Organization>
</EntityDescriptor>
""".format(
    IDP_2_ENTITY_ID,
    IDP_2_UI_INFO_EN_DISPLAY_NAME,
    IDP_2_UI_INFO_ES_DISPLAY_NAME,
    IDP_2_UI_INFO_DESCRIPTION,
    IDP_2_UI_INFO_INFORMATION_URL,
    IDP_2_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_2_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_2_SSO_BINDING.value,
    IDP_2_SSO_URL,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_URL,
)

CORRECT_XML_WITH_ONE_IDP_METADATA_WITHOUT_NAME_ID_FORMAT = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10" xml:lang="en">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor use="signing">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <KeyDescriptor use="encryption">
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{8}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <SingleSignOnService 
            Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
            Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
        <SingleSignOnService 
            Binding="{9}" 
            Location="{10}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{11}</OrganizationName>
      <OrganizationName xml:lang="es">{12}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{13}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{14}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{15}</OrganizationURL>
      <OrganizationURL xml:lang="es">{16}</OrganizationURL>
    </Organization>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    IDP_1_UI_INFO_DESCRIPTION,
    IDP_1_UI_INFO_INFORMATION_URL,
    IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_1_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    IDP_1_SSO_BINDING.value,
    IDP_1_SSO_URL,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_URL,
)

CORRECT_XML_WITH_ONE_IDP_METADATA_WITH_ONE_CERTIFICATE = """<?xml version="1.0" encoding="UTF-8"?>
<!--
     This is example metadata only. Do *NOT* supply it as is without review,
     and do *NOT* provide it in real time to your partners.

     This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptor
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
  xmlns:xml="http://www.w3.org/XML/1998/namespace"
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
  entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
        <Extensions>
            <shibmd:Scope regexp="false">example.org</shibmd:Scope>
            <mdui:UIInfo>
                <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
                <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
                <mdui:Description xml:lang="en">{3}</mdui:Description>
                <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
                <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
                <mdui:Logo height="10" width="10" xml:lang="en">{6}</mdui:Logo>
            </mdui:UIInfo>
        </Extensions>
        <KeyDescriptor>
            <ds:KeyInfo>
                    <ds:X509Data>
                        <ds:X509Certificate>
{7}
                        </ds:X509Certificate>
                    </ds:X509Data>
            </ds:KeyInfo>
        </KeyDescriptor>
        <NameIDFormat>{8}</NameIDFormat>
        <NameIDFormat>{9}</NameIDFormat>
        <SingleSignOnService 
            Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
            Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
        <SingleSignOnService 
            Binding="{10}" 
            Location="{11}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{12}</OrganizationName>
      <OrganizationName xml:lang="es">{13}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{14}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{15}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{16}</OrganizationURL>
      <OrganizationURL xml:lang="es">{17}</OrganizationURL>
    </Organization>
</EntityDescriptor>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    IDP_1_UI_INFO_DESCRIPTION,
    IDP_1_UI_INFO_INFORMATION_URL,
    IDP_1_UI_INFO_PRIVACY_STATEMENT_URL,
    IDP_1_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_1_SSO_BINDING.value,
    IDP_1_SSO_URL,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_URL,
)

CORRECT_XML_WITH_MULTIPLE_IDPS = """<?xml version="1.0" encoding="UTF-8"?>
<!--
      This is example metadata only. Do *NOT* supply it as is without review,
      and do *NOT* provide it in real time to your partners.

      This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntityDescriptors>
  <EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" 
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#" 
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0" 
    xmlns:xml="http://www.w3.org/XML/1998/namespace" 
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui" 
    entityID="{0}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
      <Extensions>
        <shibmd:Scope regexp="false">example.org</shibmd:Scope>
        <mdui:UIInfo>
          <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
          <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
        </mdui:UIInfo>
      </Extensions>
      <KeyDescriptor use="signing">
        <ds:KeyInfo>
          <ds:X509Data>
            <ds:X509Certificate>
{3}
            </ds:X509Certificate>
          </ds:X509Data>
        </ds:KeyInfo>
      </KeyDescriptor>
      <KeyDescriptor use="encryption">
        <ds:KeyInfo>
          <ds:X509Data>
            <ds:X509Certificate>
{4}
            </ds:X509Certificate>
          </ds:X509Data>
        </ds:KeyInfo>
      </KeyDescriptor>
      <NameIDFormat>{5}</NameIDFormat>
      <NameIDFormat>{6}</NameIDFormat>
      <SingleSignOnService 
        Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
        Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
      <SingleSignOnService 
        Binding="{7}" 
        Location="{8}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{9}</OrganizationName>
      <OrganizationName xml:lang="es">{10}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{11}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{12}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{13}</OrganizationURL>
      <OrganizationURL xml:lang="es">{14}</OrganizationURL>
    </Organization>
  </EntityDescriptor>
  <EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" 
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#" 
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0" 
    xmlns:xml="http://www.w3.org/XML/1998/namespace" 
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui" 
    entityID="{15}">
    <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:mace:shibboleth:1.0">
      <Extensions>
        <shibmd:Scope regexp="false">example.org</shibmd:Scope>
        <mdui:UIInfo>
          <mdui:DisplayName xml:lang="en">{16}</mdui:DisplayName>
          <mdui:DisplayName xml:lang="es">{17}</mdui:DisplayName>
        </mdui:UIInfo>
      </Extensions>
      <KeyDescriptor use="signing">
        <ds:KeyInfo>
          <ds:X509Data>
            <ds:X509Certificate>
{18}
            </ds:X509Certificate>
          </ds:X509Data>
        </ds:KeyInfo>
      </KeyDescriptor>
      <KeyDescriptor use="encryption">
        <ds:KeyInfo>
          <ds:X509Data>
            <ds:X509Certificate>
{19}
            </ds:X509Certificate>
          </ds:X509Data>
        </ds:KeyInfo>
      </KeyDescriptor>
      <NameIDFormat>{20}</NameIDFormat>
      <NameIDFormat>{21}</NameIDFormat>
      <SingleSignOnService 
        Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST-SimpleSign" 
        Location="http://idp.hilbertteam.net/idp/profile/SAML2/POST-SimpleSign/SSO"/>
      <SingleSignOnService 
        Binding="{22}" 
        Location="{23}"/>
    </IDPSSODescriptor>
    <Organization>
      <OrganizationName xml:lang="en">{24}</OrganizationName>
      <OrganizationName xml:lang="es">{25}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{26}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{27}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{28}</OrganizationURL>
      <OrganizationURL xml:lang="es">{29}</OrganizationURL>
    </Organization>
  </EntityDescriptor>
</EntityDescriptors>
""".format(
    IDP_1_ENTITY_ID,
    IDP_1_UI_INFO_EN_DISPLAY_NAME,
    IDP_1_UI_INFO_ES_DISPLAY_NAME,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_1_SSO_BINDING.value,
    IDP_1_SSO_URL,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_1_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_1_ORGANIZATION_ES_ORGANIZATION_URL,
    IDP_2_ENTITY_ID,
    IDP_2_UI_INFO_EN_DISPLAY_NAME,
    IDP_2_UI_INFO_ES_DISPLAY_NAME,
    SIGNING_CERTIFICATE,
    ENCRYPTION_CERTIFICATE,
    NAME_ID_FORMAT_1,
    NAME_ID_FORMAT_2,
    IDP_2_SSO_BINDING.value,
    IDP_2_SSO_URL,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_NAME,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_NAME,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    IDP_2_ORGANIZATION_EN_ORGANIZATION_URL,
    IDP_2_ORGANIZATION_ES_ORGANIZATION_URL,
)

INCORRECT_XML_WITH_ONE_SP_METADATA_WITHOUT_ACS_SERVICE = """<EntityDescriptor 
    xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
    entityID="{0}">
  <SPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:oasis:names:tc:SAML:1.0:protocol">
    <KeyDescriptor>
      <ds:KeyInfo>
        <ds:X509Data>
          <ds:X509Certificate>
{1}
          </ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes128-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes192-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes256-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes128-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes192-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes256-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#tripledes-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#rsa-oaep"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p"/>
    </KeyDescriptor>
  </SPSSODescriptor>
</EntityDescriptor>
""".format(
    SP_ENTITY_ID, SIGNING_CERTIFICATE
)

CORRECT_XML_WITH_ONE_SP = """<EntityDescriptor 
    xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0"
    xmlns:xml="http://www.w3.org/XML/1998/namespace"
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
    entityID="{0}">
  <SPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol urn:oasis:names:tc:SAML:1.1:protocol urn:oasis:names:tc:SAML:1.0:protocol">
    <Extensions>
      <shibmd:Scope regexp="false">example.org</shibmd:Scope>
      <mdui:UIInfo>
          <mdui:DisplayName xml:lang="en">{1}</mdui:DisplayName>
          <mdui:DisplayName xml:lang="es">{2}</mdui:DisplayName>
          <mdui:Description xml:lang="en">{3}</mdui:Description>
          <mdui:InformationURL xml:lang="en">{4}</mdui:InformationURL>
          <mdui:PrivacyStatementURL xml:lang="en">{5}</mdui:PrivacyStatementURL>
          <mdui:Logo height="10" width="10">{6}</mdui:Logo>
        </mdui:UIInfo>
      </Extensions>
    <KeyDescriptor>
      <ds:KeyInfo>
        <ds:X509Data>
          <ds:X509Certificate>
{7}
          </ds:X509Certificate>
        </ds:X509Data>
      </ds:KeyInfo>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes128-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes192-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#aes256-gcm"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes128-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes192-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes256-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#tripledes-cbc"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2009/xmlenc11#rsa-oaep"/>
      <EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#rsa-oaep-mgf1p"/>
    </KeyDescriptor>
    <AssertionConsumerService 
        Binding="{8}" 
        Location="{9}/" 
        index="1"/>
    <AssertionConsumerService 
        Binding="{8}" 
        Location="{9}" 
        index="0"/>
  </SPSSODescriptor>
  <Organization>
      <OrganizationName xml:lang="en">{10}</OrganizationName>
      <OrganizationName xml:lang="es">{11}</OrganizationName>
      <OrganizationDisplayName xml:lang="en">{12}</OrganizationDisplayName>
      <OrganizationDisplayName xml:lang="es">{13}</OrganizationDisplayName>
      <OrganizationURL xml:lang="en">{14}</OrganizationURL>
      <OrganizationURL xml:lang="es">{15}</OrganizationURL>
    </Organization>
</EntityDescriptor>
""".format(
    SP_ENTITY_ID,
    SP_UI_INFO_EN_DISPLAY_NAME,
    SP_UI_INFO_ES_DISPLAY_NAME,
    SP_UI_INFO_DESCRIPTION,
    SP_UI_INFO_INFORMATION_URL,
    SP_UI_INFO_PRIVACY_STATEMENT_URL,
    SP_UI_INFO_LOGO_URL,
    SIGNING_CERTIFICATE,
    SP_ACS_BINDING.value,
    SP_ACS_URL,
    SP_ORGANIZATION_EN_ORGANIZATION_NAME,
    SP_ORGANIZATION_ES_ORGANIZATION_NAME,
    SP_ORGANIZATION_EN_ORGANIZATION_DISPLAY_NAME,
    SP_ORGANIZATION_ES_ORGANIZATION_DISPLAY_NAME,
    SP_ORGANIZATION_EN_ORGANIZATION_URL,
    SP_ORGANIZATION_ES_ORGANIZATION_URL,
)

FEDERATED_METADATA_WITHOUT_VALID_UNTIL_ATTRIBUTE = """<?xml version="1.0" encoding="UTF-8"?>
<!--
      This is example metadata only. Do *NOT* supply it as is without review,
      and do *NOT* provide it in real time to your partners.

      This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntitiesDescriptor
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#" 
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0" 
    xmlns:xml="http://www.w3.org/XML/1998/namespace" 
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui">
</EntitiesDescriptor>
"""

FEDERATED_METADATA_VALID_UNTIL = datetime.datetime(2020, 1, 1, 0, 0, 0)

FEDERATED_METADATA_WITH_VALID_UNTIL_ATTRIBUTE = """<?xml version="1.0" encoding="UTF-8"?>
<!--
      This is example metadata only. Do *NOT* supply it as is without review,
      and do *NOT* provide it in real time to your partners.

      This metadata is not dynamic - it will not change as your configuration changes.
-->
<EntitiesDescriptor
    xmlns:ds="http://www.w3.org/2000/09/xmldsig#" 
    xmlns:shibmd="urn:mace:shibboleth:metadata:1.0" 
    xmlns:xml="http://www.w3.org/XML/1998/namespace" 
    xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui"
    validUntil="{0}">
</EntitiesDescriptor>
""".format(
    FEDERATED_METADATA_VALID_UNTIL.strftime(OneLogin_Saml2_Utils.TIME_FORMAT)
)

FEDERATED_METADATA_CERTIFICATE = """-----BEGIN CERTIFICATE-----
MIIEvjCCAyagAwIBAgIJANpi9/mkU/zoMA0GCSqGSIb3DQEBCwUAMHQxCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJNSTESMBAGA1UEBwwJQW5uIEFyYm9yMRYwFAYDVQQK
DA1JbnRlcm5ldDIuZWR1MREwDwYDVQQLDAhJbkNvbW1vbjEZMBcGA1UEAwwQbWRx
LmluY29tbW9uLm9yZzAeFw0xODExMTMxNDI5NDNaFw0zODExMTAxNDI5NDNaMHQx
CzAJBgNVBAYTAlVTMQswCQYDVQQIDAJNSTESMBAGA1UEBwwJQW5uIEFyYm9yMRYw
FAYDVQQKDA1JbnRlcm5ldDIuZWR1MREwDwYDVQQLDAhJbkNvbW1vbjEZMBcGA1UE
AwwQbWRxLmluY29tbW9uLm9yZzCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCCAYoC
ggGBAJ0+fUTzYVSP6ZOutOEhNdp3WPCPOYqnB4sQFz7IeGbFL1o0lZjx5Izm4Yho
4wNDd0h486iSkHxNf5dDhCqgz7ZRSmbusOl98SYn70PrUQj/Nzs3w47dPg9Tpb/x
y44PvNLS/rE56hPgCz/fbHoTTiJt5eosysa1ZebQ3LEyW3jGm+LGtLbdIfkynKVQ
vpp1FVeCamzdeB3ZRICAvqTYQKE1JQDGlWrEsSW0VVEGNjfbzMzr/g4l8JRdMabQ
Jig8tj3UIXnu7A2CKSMJSy3WZ3HX+85oHEbL+EV4PtpQz765c69tUIdNTJax9jQ2
1c3wL0K27HE8jSRlrXImD50R3dXQBKH+iiynBWxRPdyMBa1YfK+zZEWPbLHshSTc
9hkylQv3awmPR/+Plz5AtTpe5yss/Ifyp01wz1jt42R+6jDE+WbUjp5XDBCAjGEE
0FPaYtxjZLkmNl367bdTN12OIn/ixPNH+Z/S/4skdBB9Gc4lb2fEBywJQY0OYNOd
WOxmPwIDAQABo1MwUTAdBgNVHQ4EFgQUMHZuwMaYSJM5mlu3Wc4Ts5xq4/swHwYD
VR0jBBgwFoAUMHZuwMaYSJM5mlu3Wc4Ts5xq4/swDwYDVR0TAQH/BAUwAwEB/zAN
BgkqhkiG9w0BAQsFAAOCAYEAMr4wfLrSoPTzfpXtvL+2vrKBJNnRfuJpOYTbPKUc
DOP2QfzRlczi7suYJvd5rLiRonq8rjyPUyM8gvTfbTps+JhJ6S9mS6dTBxOV1qPZ
3Ab+XKmq8LUtguGRabKgJgmJH0+inR/wVoal7EVHcWXfij9AT8DZOXW88shc6grh
jUaFZBu/2+q8c8ee0e4ip8B+CVEnCwDKI0d+nTcSmPvAE34CNa33F+QGpXawv5yv
VvIpSaLAeFQhc/jKcnNHfy+Zi7JmSnKZiMvQCbWANQmDjHg7pGmBW9nyQcm6P2/B
0AVcEj1YTpAR8Mbh1pUdIhoB+chaNnFEIZsXeRsdbbAFpxodInlJ7WekfuvSQ6sU
EXpoyBGOeuuTmR1va8k3QeL8Wc4yNu/g5LwjmtvPrh2jBF8xujc4J6VzP8K2BjA4
xk4LnXgjHOT93dBAJhVYJkykDHwyvHUvsBHoP6lfjrt5P8zunK2mdP/AZKik+Rdt
1GGlErV2AyWShTOaDLW6NxdP
-----END CERTIFICATE-----
"""

FEDERATED_METADATA_WITH_INVALID_SIGNATURE = """<?xml version="1.0" encoding="UTF-8"?>
<EntitiesDescriptor 
  xmlns="urn:oasis:names:tc:SAML:2.0:metadata" 
  xmlns:alg="urn:oasis:names:tc:SAML:metadata:algsupport" 
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#" 
  xmlns:icmd="http://id.incommon.org/metadata" 
  xmlns:idpdisc="urn:oasis:names:tc:SAML:profiles:SSO:idp-discovery-protocol" 
  xmlns:init="urn:oasis:names:tc:SAML:profiles:SSO:request-init" 
  xmlns:mdattr="urn:oasis:names:tc:SAML:metadata:attribute" 
  xmlns:mdrpi="urn:oasis:names:tc:SAML:metadata:rpi" 
  xmlns:mdui="urn:oasis:names:tc:SAML:metadata:ui" 
  xmlns:remd="http://refeds.org/metadata" 
  xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion" 
  xmlns:shibmd="urn:mace:shibboleth:metadata:1.0" 
  xmlns:xenc="http://www.w3.org/2001/04/xmlenc#" 
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
  ID="INC20201109T195958" 
  Name="urn:mace:incommon" 
  validUntil="2020-11-23T19:59:58Z">
  <Signature xmlns="http://www.w3.org/2000/09/xmldsig#">
    <SignedInfo><CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
      <SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
      <Reference URI="#INC20201109T195958">
        <Transforms><
          Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/><Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        </Transforms><DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
        <DigestValue>q/Uhd/HYorGKDw/sPQQsXY1q94yBY8bbU1zWglDvAZA=</DigestValue>
      </Reference>
    </SignedInfo>
    <SignatureValue>GBfo3GNn4oYwZP0B+yY8omcC8XPk3iTiRzyIvlv78NpK24yu+kYRB6n9Ux2+8NxTQ8xecdiwdzlH
MxGhwlT3tyUvBxX4U6gC6OsTNtGTxymEbALkXRhNfHShtqObuTrf8gUf0IHLKh6catt2Wu3gyqV5
UE+kw1S92HDP/4UwB0nep26hq9PdRXgxdJ/GEcygJBcLvV2jWaApA1BvY2jDV7nKuBwY09RpzpGV
1QwJdZc7vS1lfpetxQc9g0W/TxQBEGcXujEEuR8sMcNismvMYrFs2EZZwoP/kX/mBVK55EseB6bT
7dVCowhuJd0t/vNuducCr8El207XNtvBGcgIgw==</SignatureValue>
<KeyInfo><X509Data><X509Certificate>MIIDgTCCAmmgAwIBAgIJAJRJzvdpkmNaMA0GCSqGSIb3DQEBCwUAMFcxCzAJBgNVBAYTAlVTMRUw
EwYDVQQKDAxJbkNvbW1vbiBMTEMxMTAvBgNVBAMMKEluQ29tbW9uIEZlZGVyYXRpb24gTWV0YWRh
dGEgU2lnbmluZyBLZXkwHhcNMTMxMjE2MTkzNDU1WhcNMzcxMjE4MTkzNDU1WjBXMQswCQYDVQQG
EwJVUzEVMBMGA1UECgwMSW5Db21tb24gTExDMTEwLwYDVQQDDChJbkNvbW1vbiBGZWRlcmF0aW9u
IE1ldGFkYXRhIFNpZ25pbmcgS2V5MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0Chd
krn+dG5Zj5L3UIw+xeWgNzm8ajw7/FyqRQ1SjD4Lfg2WCdlfjOrYGNnVZMCTfItoXTSpg4rXxHQs
ykeNiYRu2+02uMS+1pnBqWjzdPJE0od+q8EbdvE6ShimjyNn0yQfGyQKCNdYuc+75MIHsaIOAEtD
ZUST9Sd4oeU1zRjV2sGvUd+JFHveUAhRc0b+JEZfIEuq/LIU9qxm/+gFaawlmojZPyOWZ1Jlswbr
rJYYyn10qgnJvjh9gZWXKjmPxqvHKJcATPhAh2gWGabWTXBJCckMe1hrHCl/vbDLCmz0/oYuoaSD
zP6zE9YSA/xCplaHA0moC1Vs2H5MOQGlewIDAQABo1AwTjAdBgNVHQ4EFgQU5ij9YLU5zQ6K75kP
gVpyQ2N/lPswHwYDVR0jBBgwFoAU5ij9YLU5zQ6K75kPgVpyQ2N/lPswDAYDVR0TBAUwAwEB/zAN
BgkqhkiG9w0BAQsFAAOCAQEAaQkEx9xvaLUt0PNLvHMtxXQPedCPw5xQBd2VWOsWPYspRAOSNbU1
VloY+xUkUKorYTogKUY1q+uh2gDIEazW0uZZaQvWPp8xdxWqDh96n5US06lszEc+Lj3dqdxWkXRR
qEbjhBFh/utXaeyeSOtaX65GwD5svDHnJBclAGkzeRIXqxmYG+I2zMm/JYGzEnbwToyC7yF6Q8cQ
xOr37hEpqz+WN/x3qM2qyBLECQFjmlJrvRLkSL15PCZiu+xFNFd/zx6btDun5DBlfDS9DG+SHCNH
6Nq+NfP+ZQ8CGzP/3TaZPzMlKPDCjp0XOQfyQqFIXdwjPFTWjEusDBlm4qJAlQ==</X509Certificate></X509Data></KeyInfo></Signature>
</EntitiesDescriptor>
"""

PATRON_ID_REGULAR_EXPRESSION_ORG = r"(?P<patron_id>.+)@university\.org"
PATRON_ID_REGULAR_EXPRESSION_COM = r"(?P<patron_id>.+)@university\.com"

MAIL = "patron@example.com"
GIVEN_NAME = "Rosie"
SURNAME = "Nairn"
UID = "rosie.nairn"
EDU_PERSON_PRINCIPAL_NAME = "patron@example.org"

NAME_ID = "AAdzZWNyZXQxhtrjeUiJ2AIkyiOUTM6w+oRFi6ZWMol5btG40ddzFNN4ELloaTpArM1WCG1jm0DX87Tl829ptqBKrIfYw2bQstEjOaACQJljoWmbTVKWrmr4Bx60lhMFHTawA7NHq6V9gwKngwdGP2yES6tn/w=="
NAME_QUALIFIER = "http://idp.hilbertteam.net/idp/shibboleth"
NAME_FORMAT = SAMLNameIDFormat.TRANSIENT.value
SP_NAME_QUALIFIER = "http://cm.hilbertteam.net/metadata/"

JSON_DOCUMENT_WITH_SAML_SUBJECT = """{{
    "attributes": {{
        "mail": ["{0}"],
        "givenName": ["{1}"],
        "surname": ["{2}"],
        "uid": ["{3}"],
        "eduPersonPrincipalName": ["{4}"]
    }},
    "name_id": {{
        "name_id": "{5}",
        "name_qualifier": "{6}",
        "name_format": "{7}",
        "sp_name_qualifier": "{8}"
    }}
}}
""".format(
    MAIL,
    GIVEN_NAME,
    SURNAME,
    UID,
    EDU_PERSON_PRINCIPAL_NAME,
    NAME_ID,
    NAME_QUALIFIER,
    NAME_FORMAT,
    SP_NAME_QUALIFIER,
)


def strip_certificate(certificate):
    """
    Converts certificate to a one-line format

    :param certificate: Certificate in a multi-line format
    :type certificate: string

    :return: Certificate in a one-line format
    :rtype: string
    """

    return (
        certificate.replace("\n", "")
        .replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
    )


def strip_json(string_value):
    """Strip a string containing a JSON document and remove all redundant white-space symbols.

    :param string_value: String containing a JSON document
    :type string_value: str

    :return: String containing a JSON document without redundant white-space symbols
    :rtype: str
    """
    result = string_value.replace("\n", "")
    result = re.sub(r"{\s+", "{", result)
    result = re.sub(r"\s+}", "}", result)
    result = re.sub(r",\s+", ", ", result)

    return result
