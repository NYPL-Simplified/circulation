from nose.tools import set_trace
from hashlib import md5
import re
import struct
import unicodedata

class WorkIDCalculator(object):

    @classmethod
    def permanent_id(self, normalized_title, normalized_author,
                     grouping_category):
        digest = md5()
        for i in (normalized_title, normalized_author, grouping_category):
            if i == '' or i is None:
                i = '--null--'
            digest.update(i.encode("utf-8"))
        permanent_id = digest.hexdigest().zfill(32)
        permanent_id = "-".join([
            permanent_id[:8], permanent_id[8:12], permanent_id[12:16],
            permanent_id[16:20], permanent_id[20:]])
        return permanent_id

    # Strings to be removed from author names.
    authorExtract1 = re.compile("^(.+?)\\spresents.*$")
    authorExtract2 = re.compile("^(?:(?:a|an)\\s)?(.+?)\\spresentation.*$")
    distributedByRemoval = re.compile("^distributed (?:in.*\\s)?by\\s(.+)$")
    initialsFix = re.compile("(?<=[A-Z])\\.(?=(\\s|[A-Z]|$))")
    apostropheStrip = re.compile("'s")
    specialCharacterStrip = re.compile("[^\\w\\d\\s]", re.U)
    consecutiveCharacterStrip = re.compile("\\s{2,}")
    bracketedCharacterStrip = re.compile("\\[(.*?)\\]")
    commonAuthorSuffixPattern = re.compile("^(.+?)\\s(?:general editor|editor|editor in chief|etc|inc|inc\\setc|co|corporation|llc|partners|company|home entertainment)$")
    commonAuthorPrefixPattern = re.compile("^(?:edited by|by the editors of|by|chosen by|translated by|prepared by|translated and edited by|completely rev by|pictures by|selected and adapted by|with a foreword by|with a new foreword by|introd by|introduction by|intro by|retold by)\\s(.+)$")

    format_to_grouping_category = {
        "Atlas": "other",
        "Map": "other",
        "TapeCartridge": "other",
        "ChipCartridge": "other",
        "DiscCartridge": "other",
        "TapeCassette": "other",
        "TapeReel": "other",
        "FloppyDisk": "other",
        "CDROM": "other",
        "Software": "other",
        "Globe": "other",
        "Braille": "book",
        "Filmstrip": "movie",
        "Transparency": "other",
        "Slide": "other",
        "Microfilm": "other",
        "Collage": "other",
        "Drawing": "other",
        "Painting": "other",
        "Print": "other",
        "Photonegative": "other",
        "FlashCard": "other",
        "Chart": "other",
        "Photo": "other",
        "MotionPicture": "movie",
        "Kit": "other",
        "MusicalScore": "book",
        "SensorImage": "other",
        "SoundDisc": "audio",
        "SoundCassette": "audio",
        "SoundRecording": "audio",
        "VideoCartridge": "movie",
        "VideoDisc": "movie",
        "VideoCassette": "movie",
        "VideoReel": "movie",
        "Video": "movie",
        "MusicalScore": "book",
        "MusicRecording": "music",
        "Electronic": "other",
        "PhysicalObject": "other",
        "Manuscript": "book",
        "eBook": "ebook",
        "Book": "book",
        "Newspaper": "book",
        "Journal": "book",
        "Serial": "book",
        "Unknown": "other",
        "Playaway": "audio",
        "LargePrint": "book",
        "Blu-ray": "movie",
        "DVD": "movie",
        "VerticalFile": "other",
        "CompactDisc": "audio",
        "TapeRecording": "audio",
        "Phonograph": "audio",
        "pdf": "ebook",
        "epub": "ebook",
        "jpg": "other",
        "gif": "other",
        "mp3": "audio",
        "plucker": "ebook",
        "kindle": "ebook",
        "externalLink": "ebook",
        "externalMP3": "audio",
        "interactiveBook": "ebook",
        "overdrive": "ebook",
        "external_web": "ebook",
        "external_ebook": "ebook",
        "external_eaudio": "audio",
        "external_emusic": "music",
        "external_evideo": "movie",
        "text": "ebook",
        "gifs": "other",
        "itunes": "audio",
        "Adobe_EPUB_eBook": "ebook",
        "Kindle_Book": "ebook",
        "Microsoft_eBook": "ebook",
        "OverDrive_WMA_Audiobook": "audio",
        "OverDrive_MP3_Audiobook": "audio",
        "OverDrive_Music": "music",
        "OverDrive_Video": "movie",
        "OverDrive_Read": "ebook",
        "Adobe_PDF_eBook": "ebook",
        "Palm": "ebook",
        "Mobipocket_eBook": "ebook",
        "Disney_Online_Book": "ebook",
        "Open_PDF_eBook": "ebook",
        "Open_EPUB_eBook": "ebook",
        "eContent": "ebook",
        "SeedPacket": "other",
    }

    @classmethod
    def normalize_author(cls, author):
        """
        Converts to NFKD unicode.
        Strips bracket, special characters, dots out.
        Converts to single-space and strips trailing spaces.
        Strips movie studio language surrouding the possible author's name.
        Lowercases.

        Returns de-linted author's name.
        """
        if author is None or len(author) == 0:
            author = u''
        author = unicodedata.normalize("NFKD", str(author))
        author = cls.bracketedCharacterStrip.sub("", author)
        author = cls.specialCharacterStrip.sub("", author)

        groupingAuthor = cls.initialsFix.sub(" ", author)
        groupingAuthor = groupingAuthor.strip().lower();
        groupingAuthor = cls.consecutiveCharacterStrip.sub(" ", groupingAuthor)

        # extract common additional info (especially for movie studios)
        # Remove home entertainment
        for regexp in [
                cls.authorExtract1, cls.authorExtract2,
                cls.commonAuthorSuffixPattern,
                cls.commonAuthorPrefixPattern, cls.distributedByRemoval
                ]:
            match = regexp.search(groupingAuthor)
            if match:
                groupingAuthor = match.groups()[0]

        # Remove md if the author ends with md
        if groupingAuthor.endswith(" md"):
            groupingAuthor = groupingAuthor[:-3]

        if len(groupingAuthor) > 50:
            groupingAuthor = groupingAuthor[:50]
        groupingAuthor = groupingAuthor.strip()

        # TODO: I don't understand this yet.
        # groupingAuthor = RecordGroupingProcessor.mapAuthorAuthority(groupingAuthor);
        return groupingAuthor

    commonSubtitlesPattern = re.compile("^(.*?)((a|una)\\s(.*)novel(a|la)?|a(.*)memoir|a(.*)mystery|a(.*)thriller|by\\s(.+)|a novel of .*|stories|an autobiography|a biography|a memoir in books|\\d+\S*\s*ed(ition)?|\\d+\S*\s*update|1st\\s+ed.*|an? .* story|a .*\\s?book|poems|the movie|[\\w\\s]+series book \\d+|[\\w\\s]+trilogy book \\d+|large print|graphic novel|magazine|audio cd)$", re.U)

    numerics = []
    for find, replace in (
            ("1st", "first"), ("2nd", "second"), ("3rd", "third"),
            ("4th", "fourth"), ("5th", "fifth"), ("6th", "sixth"),
            ("7th", "seventh"), ("8th", "eighth"), ("9th", "ninth"),
            ("10th", "tenth")):
        numerics.append((re.compile(find), replace))


    @classmethod
    def normalize_subtitle(cls, original_title):
        if original_title == '':
            return original_title

        subtitle = original_title.replace("&#8211;", "-")
        subtitle = subtitle.replace("&", "and")

        # Remove any bracketed parts of the title
        subtitle = cls.bracketedCharacterStrip.sub("", subtitle)
        subtitle = cls.apostropheStrip.sub("s", subtitle)
        subtitle = cls.specialCharacterStrip.sub("", subtitle)
        subtitle = subtitle.lower().strip()

        subtitle = cls.consecutiveCharacterStrip.sub(" ", subtitle)

        # Remove some common subtitles that are meaningless
        match = cls.commonSubtitlesPattern.search(subtitle)
        if match:
            subtitle = match.groups()[0]
        # Normalize numeric titles
        for find, replace in cls.numerics:
            subtitle = find.sub(replace, subtitle)

        subtitle = subtitle[:175].strip()
        return subtitle

    subtitleIndicator = re.compile("[:;/=]")


    @classmethod
    def normalize_title(cls, full_title, num_non_filing_characters=0):
        """
        Converts to NFKD unicode.
        Strips bracket, special characters.
        Splits into title and subtitle portions (normalizes subtitle).
        Lowercases.
        """
        if full_title is None:
            full_title = u''
        full_title = unicodedata.normalize("NFKD", full_title)
        # Remove any bracketed parts of the title
        tmp_title = cls.bracketedCharacterStrip.sub("", full_title)
        tmp_title = cls.specialCharacterStrip.sub(
            "", full_title)

        if (num_non_filing_characters > 0
            and num_non_filing_characters < len(full_title)):
            tmp_title = full_title[:num_non_filing_characters]
        else:
            tmp_title = full_title

        tmp_title = cls.make_value_sortable(tmp_title)

        # Make sure we don't strip the entire title
        if len(tmp_title) > 0:
            # And make sure we don't get just special characters
            tmp_title = cls.specialCharacterStrip.sub(" ", tmp_title)
            tmp_title = tmp_title.lower().strip()
        if len(tmp_title) > 0:
            title = tmp_title
        else:
            # print "Just saved us from trimming %s to nothing" % full_title
            title = cls.specialCharacterStrip.sub(
                "", full_title)


        # If the title includes a : in it, take the first part as the title and the second as the subtitle
        match = cls.subtitleIndicator.search(title)
        if match:
            start = match.start()
            subtitle = cls.normalize_subtitle(title[start+1])
            title = title[:start]

            # Add the subtitle back
            if subtitle is not None and len(subtitle) > 0:
                title = title + " " + subtitle

        # Fix abbreviations
        title = cls.initialsFix.sub(" ", title)

        # Replace '&' with 'and' for better matching
        title = title.replace("&#8211;", "-")
        title = title.replace("&", "and")

        # Remove some common subtitles that are meaningless (do again here in case they were part of the title).
        match = cls.commonSubtitlesPattern.search(title)
        if match and len(match.groups()[0]) != 0:
            title = match.groups()[0]
        title = cls.apostropheStrip.sub("s", title)
        title = cls.specialCharacterStrip.sub(" ", title)
        title = title.lower()
        # Replace consecutive spaces
        title = cls.consecutiveCharacterStrip.sub(" ", title)
        title_end = 100
        if len(title) > title_end:
            title = title[:title_end]
        title = title.strip()
        if not title:
            # print "Title %s was normalized to nothing" % full_title
            title = full_title
        return title


    sortTrimmingPattern = re.compile("(?i)^(?:(?:a|an|the|el|la|\"|')\\s)(.*)$")
    @classmethod
    def make_value_sortable(cls, curtitle):
        if not curtitle:
            return ""
        sort_title = curtitle.lower()
        match = cls.sortTrimmingPattern.search(sort_title)
        if match:
            sort_title = match.groups()[0]
        sort_title = sort_title.strip()
        return sort_title
