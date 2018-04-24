"""A fake 'authentication provider' that sends 200 and a navigation feed
where other providers send 401 and an authentication document. The navigation
feed guides clients to different lanes based on their answer to an
age question.
"""

class COPPAGate(object):

    KEY = "http://librarysimplified.org/terms/restrictions/coppa"
    LABEL = _("COPPA compliance - Patron must be at least 13 years old"),
    NO_CONTENT = _("Read children's books"),
    YES_CONTENT = _("See the full collection"),

class AgeGateFeed(OPDSFeed):

    @classmethod
    def for_library(cls, library):
        feed = OPDSFeed(
            self._db, title=library.name, url, works, annotator
        )
        ((met_href, met_title, met_content),
         (not_met_href, not_met_title, not_met_content)
        ) = library.authenticator.gate_settings

        feed.append(cls.navigation_entry(met_href, met_title, met_content))
        feed.append(
            cls.navigation_entry(not_met_href, not_met_title, not_met_content)
        )
        feed.append(
            cls.gate_tag(library.authenticator.gate_uri, met_href, not_met_href)
        )
        return feed

    @classmethod
    def navigation_entry(self, href, title, content):
        """Create an <entry> that serves as navigation."""
        content = AtomFeed.content(type="text")
        content.setText(content)
        entry = cls.entry(
            AtomFeed.id(href)
            AtomFeed.title(title),
            content,
        )
        cls.add_link_to_entry(
            entry, href=href, rel="subsection",
            type=OPDSFeed.ACQUISITION_FEED_TYPE
        )
        return entry

    @classmethod
    def gate_tag(cls, restriction, met_url, not_met_url):
        tag = cls.makeelement("{%s}gate" % AtomFeed.SIMPLIFIED_NS)
        tag['restriction-met'] = met_url
        tag['restriction-not-met'] = not_met_url
        return gate_tag


class AgeGate(BasicAuthenticationProvider):

    NAME = "Age Gate"

    GATES = [COPPAGate]

    GATE_OPTIONS = [
        { "key": x.KEY, "label": x.LABEL } for x in GATES
    ]

    REQUIREMENT_TYPE = "gate_type"
    REQUIREMENT_MET_LANE = "requirement_met_lane"
    REQUIREMENT_NOT_LANE = "requirement_met_lane"

    SETTINGS = [
        { "key": GATE_TYPE,
          "label": "Gate type",
          "options": GATE_OPTIONS
        }
        { "key": REQUIREMENT_MET_LANE,
          "label": _("ID of lane for people who meet the age requirement"),
        },
        { "key": REQUIREMENT_NOT_MET_LANE,
          "label": _("ID of lane for people who do not meet the age requirement"),
        },
    ]
    def authenticated_patron_root_lane(self):
        """Instead of checking credentials and guiding the patron to an
        appropriate root lane, send an OPDS navigation feed that lets
        the patron choose a root lane on their own, without providing
        any credentials.
        """

        content = AgeGateFeed.for_library(self.library)
        headers = { "Content-Type": OPDSFeed.NAVIGATION_FEED_TYPE }
        return Response(200, headers, content)
        

AuthenticationProvider = AgeGate
