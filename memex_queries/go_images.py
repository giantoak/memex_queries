import datetime as dt
from .helpers import post_dates_for_hashed_cdr_image_id
from .helpers import cdr_ad_ids_for_hashed_cdr_image_id
from .helpers.ist.cdr import cdr_image_ids_for_cdr_ad_ids
from .helpers.lattice.sqlite import df_of_tables_for_cdr_ad_ids
from itertools import chain


def query_one(cdr_image_id):
    """
    For the general version of a given `cdr_image_id`, return the first \
    date that it was posted in an ad.

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns: `pandas.tslib.Timestamp` --
    """
    vals = post_dates_for_hashed_cdr_image_id(cdr_image_id)
    if vals is None or len(vals) == 0:
        return None
    return min(vals.post_date)


def query_two(cdr_image_id):
    """
    For the general version of a`cdr_image_id`, what phone number \
    (or numbers) posted ads using `cdr_image_id` on the first date \
    that it was posted in an ad?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns: `set` --
    """
    cdr_ad_ids = cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id)
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])
    first_date = df.post_date.min()

    return set(df.ix[(df.post_date == first_date), 'phone'].values)


def query_three(cdr_image_id, post_date):
    """
    For a given `cdr_image_id` at `post_date`, **what** phone numbers posted \
    ads with `cdr_image_id` at an earlier date?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :param str | datetime.datetime post_date: Date against which to check
    :returns: `set` --
    """
    cdr_ad_ids = cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id)
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])

    if isinstance(post_date, (str, unicode)):
        from pandas import to_datetime
        post_date = to_datetime(post_date)

    return set(df.ix[df.post_date < post_date, 'phone'])


def query_four(cdr_image_id, post_date):
    """
    For a given `cdr_image_id` at `post_date`,
    HOW MANY phone numbers posted ads with I at an earlier date?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :param str | datetime.datetime post_date: Date against which to check
    :returns: `int` --
    """
    return len(query_three(cdr_image_id, post_date))


def query_five(cdr_image_id):
    """
    For a given cdr_image_id, what phone numbers posted ads using it?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns: `set` --
    """
    cdr_ad_ids = cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id)
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_phone'])
    return set(df.phone)


def query_six(cdr_image_id):
    """
    For a given cdr_image_id, how many phone numbers posted ads using it?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns: `int` --
    """
    return len(query_five(cdr_image_id))


def query_seven(cdr_image_id, post_date, phone_number=None):
    """
    For a given `cdr_image_id` posted on `post_date` by `phone_number`
    what phone numbers **other than `phone_number`** posted ads with \
    `cdr_image_id` at an earlier date?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :param str | unicode | datetime.datetime post_date: Date against which to check
    :param str | unicode phone_number:
    :returns: --
    """
    cdr_ad_ids = cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id)
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])

    if isinstance(post_date, (str, unicode)):
        post_date = dt.datetime(post_date)

    if phone_number is None:
        phone_number = df.ix[df.post_date == post_date, 'phone']

    return set(df.ix[(df.phone != phone_number) & df.post_date < post_date, 'phone'])


def query_eight(cdr_image_id, post_date, phone_number=None):
    """
    For a given `cdr_image_id` posted by `phone_number` at `post_date`, \
    **how many** phone numbers other than `phone_number`-posted ads with \
    `cdr_image_id` at an earlier date?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :param str | unicode | datetime.datetime post_date: Date against which to check
    :param str | unicode phone_number:
    :returns: `int` --
    """
    return len(query_seven(cdr_image_id, post_date, phone_number))


def query_nine(cdr_ad_id, phone_number=None):
    """
    For a given `cdr_ad_id` posted by `phone_number` what fraction of the \
    images were first used in ads posted by by phone numbers other than P?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :param str | unicode phone_number: The phone_number to use. if None, use all of \
    the numbers extracted from the ad.
    :returns: --
    """
    cdr_image_ids = cdr_image_ids_for_cdr_ad_ids([cdr_ad_id])[cdr_ad_id]

    cdr_ad_ids = list(set(chain(*[cdr_ad_ids_for_hashed_cdr_image_id(cdr_image_id)
                                  for cdr_image_id in cdr_image_ids])
                          ))
    df = df_of_tables_for_cdr_ad_ids(cdr_ad_ids, ['dd_id_to_phone', 'dd_id_to_post_date'])

    if phone_number is None:
        phone_numbers = df.ix[df.cdr_id == cdr_ad_id, 'phone'].tolist()
    else:
        phone_numbers = [phone_number]


def query_ten(cdr_ad_id):
    """
    For all of the images affiliated with a given `cdr_ad_id`, if you find which \
    phone number originated each ad, how many total sources does the ad have?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns: --
    """


def query_eleven(cdr_image_id, epochtime=None):
    """
    For a given `cdr_image_id` posted at `epochtime`, how long has it been since \
    the image was last posted?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :param int epochtime: if None, assume we're talking about the gap between \
    the most and second-most recent instance. If an int, find the first \
    timestamp BEFORE the one provided.
    :returns: `int` -- epochtime since the last posting
    """
    ad_timestamps = sorted(post_dates_for_hashed_cdr_image_id(cdr_image_id))

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


def query_twelve(cdr_image_id):
    """
    For a given `cdr_image_id`, what is its radius of gyration?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns:  --
    """


def query_thirteen():
    """
    Estimate \gamma for arrival rate under Poisson or some other similar thing

    :returns:  --
    """


def query_fourteen(cdr_ad_id):
    """
    For a given cdr_ad_id posted at time T,
    what fraction of the images have not been used in prior ads?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_fifteen(cdr_ad_id):
    """
    For a given `cdr_ad_id` posted at time T, what is the mean number of
    times images in the ad have appeared before?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_sixteen(cdr_ad_id):
    """
    For a given `cdr_ad_id`, what is the mean amount time since ads in the \
    photos first appeared?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_seventeen(cdr_image_id):
    """
    For a given `cdr_image_id`, what is the median price in all ads using \
    this photo?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns:  --
    """


def query_eighteen(cdr_image_id):
    """
    For a given `cdr_image_id`, what proportion of ads including that image offer \
    outcall services?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns:  --
    """


def query_nineteen(cdr_image_id):
    """
    For a given `cdr_image_id`, what is the Lat-long of the centroid defined by \
    the locations provided for the advertisements in which I has been used?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns:  --
    """


def query_twenty(cdr_image_id):
    """
    For a given `cdr_image_id` used in an ad at time T, what is the largest \
    Photo Gap (query_eleven) that has been seen for any use of the I in \
    prior ads?

    :param str | unicode cdr_image_id: The CDR ID of the image to retrieve
    :returns:  --
    """


def query_twenty_one():
    """
    query_seven and query_seventeen are general attempts at finding time \
    period clusters for images.

    A specific time-based cluster of images would be useful.
    :returns:  --
    """


def query_twenty_two(cdr_ad_id):
    """
    For a given `ad_id` with a set of images (I), what is the first date on \
    which an ad using exactly (I) appeared?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_twenty_three(cdr_ad_id):
    """
    For a given `ad_id` with a set of images (I), what is the first date on \
    which an ad using (I) or a superset of (I) appeared?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_twenty_four(cdr_ad_id):
    """
    For a given `cdr_ad_id` with a set of images (I) posted at time T, how \
    many prior ads have used exactly (I)?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """


def query_twenty_five(cdr_ad_id):
    """
    For a given `cdr_ad_id` with a set of images (I) posted at time T, how \
    many prior ads have used (I) or a superset of (I)?

    :param str | unicode cdr_ad_id: The CDR ID of an ad
    :returns:  --
    """
