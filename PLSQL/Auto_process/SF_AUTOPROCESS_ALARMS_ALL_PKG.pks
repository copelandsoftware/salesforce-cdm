create or replace PACKAGE     SF_AUTOPROCESS_ALARMS_ALL_PKG AS

PROCEDURE  Validate_Alarm_Proc(
                               admFlag IN  VARCHAR2, 
                               errbuf  OUT VARCHAR2,
                               retcode OUT VARCHAR2
                              );
function get_timezones_SERVER_TO_GMT(i_timevalue IN DATE) return date;

function get_timezones_GMT_TO_SERVER(i_timevalue IN DATE) return date;

function get_timezones(i_timevalue in date) return date;

END SF_AUTOPROCESS_ALARMS_ALL_PKG;