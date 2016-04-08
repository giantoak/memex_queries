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



