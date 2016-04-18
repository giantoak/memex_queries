from HelperFuncs import all_timestamps_for_cdr_image
from HelperFuncs import all_ad_ids_for_cdr_image_id
from HelperFuncs import dd_df_from_sqlite_tables
from HelperFuncs import dd_id


def query_one(image_id, es=None):
    """
    For a given image image_id, return the first date (in the CDR) that it appeared)
    Currently: Get the image from the CDR
    Hit Svebor's table to get the image hash.
    If that doesn't work, hit the image binary and get the SHA1 yourself.
    Hit Svebor's table for the list of related ads. If that doesn't work,
    give up.
    Hit Svebor's table for a list of timestamps.
    If that doesn't work, hit elastic
    :todo: Use Lattice's extracted post data, not Svebor's table.
    :todo: Make default more efficient by hitting elastic with all ads at once
    :param str image_id: The CDR ID of the image to retrieve
    :param elasticsearch.Elasticsearch es: the elasticsearch index to search
    """
    return min(all_timestamps_for_cdr_image(image_id, es))


def query_two(image_id):
    """
    For a given image_id, what phone number (or numbers) posted ads using image_id
    on the first date (in the CDR) that it appeared?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """
    dd_ad_ids = [dd_id(x) for x in all_ad_ids_for_cdr_image_id(image_id)]
    df = dd_df_from_sqlite_tables(dd_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])
    first_date = df.post_date.min()

    return set(df.ix[(df.post_date == first_date), 'phone'].values)


def query_three(image_id, timestamp):
    """
    For a given image_id at timestamp
    WHAT phone numbers posted ads with image_id at an earlier date?
    :param str image_id: The CDR ID of the image to retrieve
    :param timestamp:
    :return:
    """
    dd_ad_ids = [dd_id(x) for x in all_ad_ids_for_cdr_image_id(image_id)]
    df = dd_df_from_sqlite_tables(dd_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])
    return set(df.ix[df.post_date < timestamp, 'phone'])


def query_four(image_id, timestamp):
    """
    For a given image_id ~~posted by phone_number~~ at timestamp,
    HOW MANY phone numbers posted ads with I at an earlier date?
    :param str image_id: The CDR ID of the image to retrieve

    :return:
    """
    return len(query_three(image_id, timestamp))


def query_five(image_id):
    """
    For a given image_id, what phone numbers posted ads using it?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """
    dd_ad_ids = all_ad_ids_for_cdr_image_id(image_id)
    return set(dd_df_from_sqlite_tables(dd_ad_ids, ['dd_id_to_phone']).phone)


def query_six(image_id):
    """
    For a given image_id, how many phone numbers posted ads using it?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """
    return len(query_five(image_id))


def query_seven(image_id, phone_number, timestamp):
    """
    For a given image_id posted by phone_number at timestamp
    what phone numbers OTHER THAN PHONE_NUMBER posted ads with image_id at an earlier date?
    :param str image_id: The CDR ID of the image to retrieve
    :param str phone_number:
    :return:
    """
    dd_ad_ids = all_ad_ids_for_cdr_image_id(image_id)
    df = dd_df_from_sqlite_tables(dd_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])
    return set(df.ix[(df.phone != phone_number) & df.timestamp < timestamp, 'phone'].values)


def query_eight(image_id, phone_number, timestamp):
    """
    For a given image_id posted by phone_number at timestamp,
    how many phone numbers other than P posted ads with I at an earlier date?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_nine(ad_id, phone_number=None):
    """
    For a given ad_id posted by phone_number
    what fraction of the images were first used in ads posted by by phone numbers other than P?
    :param str ad_id: The CDR ID of an ad
    :param str phone_number:
    :return:
    """


def query_ten(ad_id):
    """
    For all of the images affiliated with a given ad_id,
    if you find which phone number originated each ad, how many total sources does the ad have?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_eleven(image_id, epochtime=None, es=None):
    """
    For a given image posted at a certain time, how long has it been since the image was last posted?
    :param str image_id: The CDR ID of the image to retrieve
    :param int epochtime: if None, assume we're talking about the gap between the most and second-most recent instance
    if an int, find the first timestamp BEFORE the one provided.
    :param elasticsearch.Elasticsearch es:
    :return int: epochtime since the last posting
    """
    ad_timestamps = sorted(all_timestamps_for_cdr_image(image_id, es))

    if epochtime is None:
        # If there's been no postings, the gap is infinite
        if len(ad_timestamps) < 1:
            return None

        # If there's been one posting, the gap is 0
        if len(ad_timestamps) < 2:
            return 0

        return ad_timestamps[-1] - ad_timestamps[-2]

    for ts in ad_timestamps[::-1]:
        if ts < epochtime:
            return epochtime - ts

    return 0


def query_twelve(image_id):
    """
    For a given image_id, what is its radius of gyration?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_thirteen():
    """
    Estimate \gamma for arrival rate under Poisson or some other similar thing
    :return:
    """


def query_fourteen(ad_id):
    """
    For a given ad_id posted at time T,
    what fraction of the images have not been used in prior ads?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_fifteen(ad_id):
    """
    For a given ad_id posted at time T, what is the mean number of times images in the ad have appeared before?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_sixteen(ad_id):
    """
    For a given ad_id, what is the mean amount time since ads in the photos first appeared?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_seventeen(image_id):
    """
    For a given image_id, what is the median price in all ads using this photo?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_eighteen(image_id):
    """
    For a given image_id, what proportion of ads including that image offer outcall services?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_nineteen(image_id):
    """
    For a given image_id,
    what is the Lat-long of the centroid defined by the locations provided for the advertisements
    in which I has been used?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_twenty(image_id):
    """
    For a given image_id used in an ad at time T,
    what is the largest Photo Gap (query_eleven) that has been seen for any use of the I in prior ads?
    :param str image_id: The CDR ID of the image to retrieve
    :return:
    """


def query_twenty_one():
    """
    query_seven and query_seventeen are general attempts at finding time period clusters for images.
    A specific time-based cluster of images would be useful.
    :return:
    """


def query_twenty_two(ad_id):
    """
    For a given ad_id
    with a set of images (I),
    what is the first date on which an ad using exactly (I) appeared?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_twenty_three(ad_id):
    """
    For a given ad_id with a set of images (I),
    what is the first date on which an ad using (I) or a superset of (I) appeared?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_twenty_four(ad_id):
    """
    For a given ad_id with a set of images (I) posted at time T,
    how many prior ads have used exactly (I)?
    :param str ad_id: The CDR ID of an ad
    :return:
    """


def query_twenty_five(ad_id):
    """
    For a given ad_id with a set of images (I) posted at time T,
    how many prior ads have used (I) or a superset of (I)?
    :param str ad_id: The CDR ID of an ad
    :return:
    """