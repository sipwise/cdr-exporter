0 0 * * *   root    . /etc/default/ngcp-roles; if /usr/sbin/ngcp-check-active -q && [ "$NGCP_IS_MGMT" = "yes" ] ; then /usr/sbin/ngcp-event-exporter >/dev/null; fi
