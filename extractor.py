"""
Extracts data from the Jellyfin database (SQLite)
"""

import sqlite3
import json
import logging
import uuid
from collections import defaultdict

log = logging.getLogger("extract")

DATA = {}
JELLYFIN_TYPES = {
    'Emby.Server.Implementations.Playlists.PlaylistsFolder': 'folder-playlist',
    'MediaBrowser.Controller.Channels.Channel': 'channel',
    'MediaBrowser.Controller.Entities.AggregateFolder': 'folder-aggregate',
    'MediaBrowser.Controller.Entities.Audio.Audio':'audio',
    'MediaBrowser.Controller.Entities.Audio.MusicAlbum': 'music-album',
    'MediaBrowser.Controller.Entities.Audio.MusicArtist': 'music-artist',
    'MediaBrowser.Controller.Entities.Audio.MusicGenre': 'genre-music',
    'MediaBrowser.Controller.Entities.CollectionFolder': 'folder-collection',
    'MediaBrowser.Controller.Entities.Folder': 'folder',
    'MediaBrowser.Controller.Entities.Genre': 'genre',
    'MediaBrowser.Controller.Entities.Movies.BoxSet': 'box-set',
    'MediaBrowser.Controller.Entities.Movies.Movie': 'movie',
    'MediaBrowser.Controller.Entities.Person': 'person',
    'MediaBrowser.Controller.Entities.Studio': 'studio',
    'MediaBrowser.Controller.Entities.TV.Episode': 'tv-episode',
    'MediaBrowser.Controller.Entities.TV.Season': 'tv-season',
    'MediaBrowser.Controller.Entities.TV.Series': 'tv-series',
    'MediaBrowser.Controller.Entities.UserRootFolder': 'folder-root',
    'MediaBrowser.Controller.Entities.UserView': 'view-user',
    'MediaBrowser.Controller.Entities.Video': 'video',
    'MediaBrowser.Controller.Playlists.Playlist': 'playlist',
}

SQLITE_DATABASE = 'jellyfin.db'
SQLITE_FIELDS_MAPPER = {
    'Album': 'album',
    'AlbumArtists': 'albumArtists',
    'Artists': 'artists',
    'Audio': 'audio',
    'ChannelId': 'channelId',
    'CleanName': 'cleanName',
    'CommunityRating': 'communityRating',
    'CriticRating': 'criticRating',
    'CustomRating': 'customRating',
    'data': 'data',
    'DateCreated': 'dateCreated',
    'DateLastMediaAdded': 'dateLastMediaAdded',
    'DateLastRefreshed': 'dateLastRefreshed',
    'DateLastSaved': 'dateLastSaved',
    'DateModified': 'dateModified',
    'EndDate': 'endDate',
    'EpisodeTitle': 'episodeTitle',
    'ExternalId': 'externalId',
    'ExternalSeriesId': 'externalSeriesId',
    'ExternalServiceId': 'externalServiceId',
    'ExtraIds': 'extraIds',
    'ExtraType': 'extraType',
    'ForcedSortName': 'forcedSortName',
    'Genres': 'genres',
    'Height': 'height',
    'Images': 'images',
    'IndexNumber': 'indexNumber',
    'InheritedParentalRatingValue': 'inheritedParentalRatingValue',
    'IsFolder': 'isFolder',
    'IsInMixedFolder': 'isInMixedFolder',
    'IsLocked': 'isLocked',
    'IsMovie': 'isMovie',
    'IsRepeat': 'isRepeat',
    'IsSeries': 'isSeries',
    'IsVirtualItem': 'isVirtualItem',
    'LockedFields': 'lockedFields',
    'MediaType': 'mediaType',
    'Name': 'name',
    'OfficialRating': 'officialRating',
    'OriginalTitle': 'originalTitle',
    'Overview': 'overview',
    'OwnerId': 'ownerId',
    'ParentIndexNumber': 'parentIndexNumber',
    'Path': 'path',
    'PreferredMetadataCountryCode': 'preferredMetadataCountryCode',
    'PreferredMetadataLanguage': 'preferredMetadataLanguage',
    'PremiereDate': 'premiereDate',
    'PresentationUniqueKey': 'uniqueKey',
    'PrimaryVersionId': 'primaryVersionId',
    'ProductionLocations': 'productionLocations',
    'ProductionYear': 'productionYear',
    'ProviderIds': 'externalIds',
    'RunTimeTicks': 'runTimeTicks',
    'SeasonId': 'seasonId',
    'SeasonName': 'seasonName',
    'SeriesId': 'seriesId',
    'SeriesName': 'seriesName',
    'SeriesPresentationUniqueKey': 'seriesPresentationUniqueKey',
    'ShowId': 'showId',
    'Size': 'size',
    'SortName': 'sortName',
    'StartDate': 'startDate',
    'Studios': 'studios',
    'Tagline': 'tagline',
    'Tags': 'tags',
    'TopParentId': 'topParentId',
    'TotalBitrate': 'totalBitrate',
    'TrailerTypes': 'trailerTypes',
    'type': 'type',
    'UnratedType': 'unratedType',
    'UserDataKey': 'userDataKey',
    'Width': 'width',

    'quote(ParentId)': 'parentId',
    'quote(guid)': 'guid',
}


def toUUID(s: str):
    if not s or s == 'NULL' or len(s) < 4:
        log.debug(s)
        return None
    return uuidFormat(s[2:-1])


def uuidFormat(s: str):
    return str(uuid.UUID(s))


class Table(object):
    def __init__(self, db, table):
        self.db = db
        self.table = table

    def serialize(self, item):
        return item


class AncestorTable(Table):
    def __init__(self, db, table='AncestorIds'):
        super().__init__(db, table)
        self.sql = f"""
            SELECT
                quote(AncestorId) as AncestorId,
                quote(ItemId) as ItemId
            FROM {self.table}
        """

    def serialize(self):
        data = self.db.execute(self.sql).fetchall()
        response = defaultdict(list)

        for item in data:
            response[toUUID(item['ItemId'])].append(toUUID(item['AncestorId']))

        return json.loads(json.dumps(response))


class ItemValues(Table):
    def __init__(self, db, table='ItemValues'):
        super().__init__(db, table)
        self.sql = f"""
            SELECT
                Value,
                CleanValue,
                Type,
                quote(ItemId) as ItemId
            FROM {self.table}
        """

    def serialize(self):
        data = self.db.execute(self.sql).fetchall()
        response = defaultdict(list)

        for item in data:
            response[toUUID(item['ItemId'])].append({
                'type': item['Type'],
                'value': item['Value'],
                'formatted': item['CleanValue'],
            })

        return response


def toList(obj, field, sep):
    if not obj[field]:
        return []
    return obj[field].split(sep)


def toDict(obj, field, sep):
    if not obj[field]:
        return []
    data = {}
    for item in obj[field].split(sep):
        k, v = item.split('=')
        data[k] = v
    return data


def toInt(obj, default):
    try:
        return int(obj)
    except ValueError:
        return default


class ParseFactory(object):
    def __init__(self, fields):
        self.fields = fields

    def get_strategy(self, entity_type):
        # TODO: move to dictionary
        if entity_type == 'person':
            return ParsePersonStragegy(self.fields)
        if entity_type == 'tv-episode':
            return ParseTvEpisode(self.fields)
        if entity_type == 'tv-season':
            return ParseTvSeason(self.fields)
        if entity_type == 'tv-series':
            return ParseTvSeries(self.fields)
        if entity_type in ['genre-music', 'genre']:
            return ParseGenreStrategy(self.fields)
        if entity_type == 'box-set':
            return BoxSetStrategy(self.fields)
        return ParseStrategy(self.fields)


class ParseStrategy(object):
    """
    How should I parse the data?
    """

    def __init__(self, fields):
        self.fields = fields

    def _parse_bytes(self, item: sqlite3.Row, key, default=None):
        try:
            return item[key].decode('utf-8')
        except UnicodeDecodeError:
            return default

    def blob_fields(self):
        """
        SQLite BLOB fields
        """
        return (
            'guid',
            'parentId',
        )

    def date_fields(self):
        """
        Date fields
        """
        return (
            'dateCreated',
            'dateModified',
        )

    def dict_fields(self):
        return (
            'externalIds',
        )

    def list_fields(self):
        """
        Text fields separated by '|'
        """
        return ()

    def text_fields(self):
        """
        Text fields
        """
        return (
            'path',
            'type',
        )

    def get_names(self, item: sqlite3.Row):
        return [{
            'name': item['name'].strip(),
            # TODO: Not everything is in english ;)
            'locale': 'en',
        }]

    def get_images(self, item: sqlite3.Row):
        images = []
        for data in toList(item, 'images', '|'):
            image = data.split('*')
            images.append({
                'path': image[0],
                'type': image[-3],
                'height': int(image[-2]),
                'width': int(image[-1]),
            })
        return images

    def parse_trailers(self, data: dict):
        if not hasattr(data, 'files'):
            data['files'] = defaultdict(list)

        try:
            for item in data['data']['RemoteTrailers']:
                data['files']['trailers'].append({
                    'remote': True,
                    'url': item['Url']
                })

            del data['data']['RemoteTrailers']
        except KeyError:
            pass
        return data

    def parse_common_fields(self, item: sqlite3.Row, entity: dict):
        for key in self.fields.values():
            if key in self.text_fields():
                entity[key] = item[key]
            elif key in self.blob_fields():
                entity[key] = toUUID(item[key])
            elif key in self.date_fields():
                entity[key] = item[key]
            elif key in self.list_fields():
                entity[key] = toList(item, key, '|')
            elif key in self.dict_fields():
                entity[key] = toDict(item, key, '|')
        return self.parse_data(item, entity)

    def parse_data(self, item: sqlite3.Row, entity: dict):
        if isinstance(item['data'], bytes):
            entity['data'] = json.loads(item['data'].decode('utf-8'))
        elif item['data']:
            entity['data'] = json.loads(item['data'])
        return entity

    def parse(self, item: sqlite3.Row):
        entity = {}
        for key in self.fields.values():
            if key in self.blob_fields():
                entity[key] = toUUID(item[key])
            elif isinstance(item[key], bytes):
                entity[key] = self._parse_bytes(item, key)
            else:
                entity[key] = item[key]

        return entity


class BoxSetStrategy(ParseStrategy):

    def text_fields(self):
        return (
            'path',
            'type',
            'productionYear',
            'premiereDate',
            'officialRating',
        )

    def list_fields(self):
        return (
            'genres',
            'studios',
        )

    def delete_data_keys(self, entity, keys):
        for k in keys:
            try:
                del entity['data'][k]
            except KeyError:
                pass
        return entity

    def parse_data(self, item: sqlite3.Row, entity: dict):
        entity = super().parse_data(item, entity)
        entity = self.parse_trailers(entity)
        entity['files']['images'] = self.get_images(item)

        if entity['data']['LinkedChildren']:
            entity['children'] = defaultdict(dict)
            for item in entity['data']['LinkedChildren']:
                entity['children'][uuidFormat(item['ItemId'])] = {
                    'path': item['Path'],
                }
            self.delete_data_keys(entity, ['LinkedChildren'])

        return self.delete_data_keys(entity, [
            'Width', 'Height', 'IsHD', 'IsShortcut', 'IsRoot'
        ])

    def parse(self, item: sqlite3.Row):
        entity = {}
        entity['names'] = self.get_names(item)

        return self.parse_common_fields(item, entity)


class ParsePersonStragegy(ParseStrategy):

    def parse(self, item: sqlite3.Row):
        entity = self.parse_common_fields(item, {})
        entity['names'] = self.get_names(item)

        return entity


class ParseGenreStrategy(ParseStrategy):
    def parse(self, item: sqlite3.Row):
        entity = {}
        entity['files'] = defaultdict(list)
        entity['files']['images'] = self.get_images(item)
        entity['names'] = self.get_names(item)

        return self.parse_common_fields(item, entity)


class ParseTvStrategy(ParseStrategy):

    def list_fields(self):
        return (
            'genres',
            'studios',
        )

    def text_fields(self):
        return (
            'productionYear',
            'premiereDate',
            'officialRating',
            'path',
            'tags',
        )

    def parse(self, item: sqlite3.Row):
        entity = self.parse_common_fields(item, {})
        entity = self.parse_trailers(entity)
        entity['files']['images'] = self.get_images(item)
        entity['overview'] = {'en': item['overview']}

        return entity


class ParseTvEpisode(ParseTvStrategy):
    def text_fields(self):
        return (
            'indexNumber',
            'parentIndexNumber',
            'productionYear',
            'premiereDate',
            'officialRating',
            'path',
            'type',
            'tags',
        )

    def parse_subtitles(self, entity: dict):
        if entity['data']['SubtitleFiles']:
            for sub in entity['data']['SubtitleFiles']:
                entity['files']['subtitles'].append({'path': sub})

        for k in ['IsHD', 'Width', 'Height', 'Size', 'SubtitleFiles']:
            try:
                del entity['data'][k]
            except KeyError:
                pass
        return entity

    def parse(self, item: sqlite3.Row):
        entity = super().parse(item)
        entity['files']['videos'] = [{
            'path': entity['path'],
            'isHD': entity['data']['IsHD'],
            'size': entity['data'].get('Size'),
            'width': entity['data']['Width'],
            'height': entity['data']['Height'],
        }]

        entity = self.parse_subtitles(entity)

        return entity


class ParseTvSeries(ParseTvStrategy):
    pass


class ParseTvSeason(ParseTvStrategy):
    pass


class Items(Table):
    def __init__(self, db, table='TypedBaseItems'):
        super().__init__(db, table)
        self.types = JELLYFIN_TYPES
        self.fields = SQLITE_FIELDS_MAPPER

        self.sql = f"""
            SELECT {','.join([f'{k} as {v}' for k, v in self.fields.items()])}
            FROM {self.table}
            WHERE type in ({','.join([f"'{t}'" for t in self.types.keys()])})
            ORDER BY type, name
            """
        log.debug(f"Executing SQL: {self.sql}")

    def serialize(self):
        data = self.db.execute(self.sql).fetchall()
        response = defaultdict(dict)
        factory = ParseFactory(self.fields)

        for item in data:
            item_type = self.types[item['type']]
            parser = factory.get_strategy(item_type)
            response[item_type][toUUID(item['guid'])] = parser.parse(item)

        return response


class PeopleTable(Table):
    def __init__(self, db, table='People'):
        super().__init__(db, table)
        self.sql = f"""
        SELECT
            ListOrder,
            Name,
            PersonType,
            Role,
            quote(ItemId) as ItemId
        FROM {self.table}
        """

    def serialize(self):
        data = self.db.execute(self.sql).fetchall()
        response = defaultdict(list)

        for item in data:
            response[toUUID(item['ItemId'])].append({
                'name': item['Name'],
                'type': item['PersonType'],
                'role': item['Role'],
                'order': item['ListOrder'],
            })

        return response


serializers = {
    'eav': ItemValues,
    'items': Items,
    'parents': AncestorTable,
    'people': PeopleTable,
}

con = sqlite3.connect(SQLITE_DATABASE)
con.row_factory = sqlite3.Row

for name, klass in serializers.items():
    table = klass(con)
    DATA[name] = table.serialize()

print('redump')
DATA = json.loads(json.dumps(DATA))
print('redump complete')

with open('data.json', 'w', encoding="utf-8") as outfile:
    json.dump(DATA, outfile, ensure_ascii=False, indent=2)
