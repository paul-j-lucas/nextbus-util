#! /usr/bin/env python
##
#   nbclosest.py:   Splunk custom search command for filtering NextBus data.
#   Version:        1.0
#
#   Author:         Paul J. Lucas
##

import operator
import sys

from splunklib.searchcommands import dispatch, Configuration, EventingCommand
from splunklib.searchcommands.decorators import ConfigurationSetting

###############################################################################

K_STAG  = 'stop_tag'
K_TIME  = '_time'
K_VDIST = 'vehicle_distance'
K_VID   = 'vehicle_id'

###############################################################################

@Configuration()
class NextBusClosestStop( EventingCommand ):
    """ Filters NextBus records to only those where a vehicle is at its
        closest distance to a given stop.

    ##Syntax

    .. code-block::
        nbclosest

    ##Description

    The :code:`nbclosest` command filters NextBus records to only those where a
    vehicle is at its closest distance to a given stop.
    """

    class ConfigurationSettings( EventingCommand.ConfigurationSettings ):
        required_fields = ConfigurationSetting(
            value=[ K_TIME, K_VID, K_VDIST, K_STAG ]
        )


    def __init__( self ):
        super( NextBusClosestStop, self ).__init__()
        self.vdict = { }


    def transform( self, records ):
        for rec in records:
            vid = rec[ K_VID ]
            if vid not in self.vdict:
                self.vdict[ vid ] = rec
            else:
                old_rec = self.vdict[ vid ]
                old_stop = old_rec[ K_STAG ]
                new_stop =     rec[ K_STAG ]
                if new_stop == old_stop:
                    old_dist = int( old_rec[ K_VDIST ] )
                    new_dist = int(     rec[ K_VDIST ] )
                    if new_dist <= old_dist:
                        self.vdict[ vid ] = rec
                else:
                    yield old_rec
                    self.vdict[ vid ] = rec

        remaining_recs = self.vdict.values()
        for rec in sorted( remaining_recs, key=operator.itemgetter( K_TIME ) ):
            yield rec


dispatch( NextBusClosestStop, sys.argv, sys.stdin, sys.stdout, __name__ )

###############################################################################
# vim:set et sw=4 ts=4:
