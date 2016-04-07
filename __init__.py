from HelperFuncs import hbase_row_value
from HelperFuncs import image_hash
from HelperFuncs import timestamp_for_cdr_id


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
    :param str image_id: The image_id to retrieve
    :param elasticsearch.Elasticsearch es: the elasticsearch index to search
    """
    cur_hash = image_hash(image_id, es)

    # Check Svebor's table for a copy of the hashed image, and get the other
    # images.
    ad_ids = hbase_row_value('ht_images_infos_2016',
                             cur_hash,
                             'info:all_parent_ids')

    if ad_ids is None:
        # We don't have a fallback for this, as yet.
        return None

    return min([timestamp_for_cdr_id(ad_id) for ad_id in ad_ids.split(',')])
