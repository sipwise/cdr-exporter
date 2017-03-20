0 0 * * *   root    . /etc/default/ngcp-roles; if /usr/sbin/ngcp-check_active -q && [ "$NGCP_IS_MGMT" = "yes" ] ; then /usr/sbin/event-exporter >/dev/null; fi
