#! /usr/bin/env python
##
#   nbclosest.py:   Splunk custom search command for filtering NextBus data.
#   Version:        1.0
#
#   Author:         Paul J. Lucas
##

import csv
import operator
import sys

###############################################################################

K_STAG  = 'stop_tag'
K_TIME  = '_time'
K_VDIST = 'vehicle_distance'
K_VID   = 'vehicle_id'

###############################################################################

vehicle_dict = { }

try:
    reader = csv.DictReader( sys.stdin )
    headers = reader.fieldnames
    writer = csv.DictWriter( sys.stdout, headers )
    writer.writeheader()

    for row in reader:
        vid = row[ K_VID ]
        if vid not in vehicle_dict:
            vehicle_dict[ vid ] = row
        else:
            old_row = vehicle_dict[ vid ]
            old_stop = old_row[ K_STAG ]
            new_stop =     row[ K_STAG ]
            if new_stop == old_stop:
                old_dist = int( old_row[ K_VDIST ] )
                new_dist = int(     row[ K_VDIST ] )
                if new_dist <= old_dist:
                    vehicle_dict[ vid ] = row
            else:
                writer.writerow( old_row )
                vehicle_dict[ vid ] = row

    remaining_rows = vehicle_dict.values()
    for row in sorted( remaining_rows, key=operator.itemgetter( K_TIME ) ):
        writer.writerow( row )

except Exception as e:
    import traceback
    stack = traceback.format_exc()
    print >>sys.stderr, "Unhandled exception: %s; %s" % (e, stack)

###############################################################################
# vim:set et sw=4 ts=4:
