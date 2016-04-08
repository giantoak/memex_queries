from HelperFuncs import all_timestamps_for_cdr_image


def query_one(image_id, es=None):
    """
    Goal: For a given image image_id, returns the first date (in the CDR) that it appeared)
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


def query_eleven(image_id, epochtime=None, es=None):
    """
    Goal: For a given image posted at a certain time, how long has it been since the image was last posted?
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

