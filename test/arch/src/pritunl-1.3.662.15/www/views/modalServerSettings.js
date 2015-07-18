define([
  'jquery',
  'underscore',
  'backbone',
  'select',
  'views/modal',
  'text!templates/modalServerSettings.html'
], function($, _, Backbone, Select, ModalView, modalServerSettingsTemplate) {
  'use strict';
  var ModalServerSettingsView = ModalView.extend({
    className: 'server-settings-modal',
    template: _.template(modalServerSettingsTemplate),
    title: 'Server Settings',
    okText: 'Save',
    loadingMsg: 'Saving server...',
    errorMsg: 'Failed to saving server, server error occurred.',
    hasAdvanced: true,
    events: function() {
      return _.extend({
        'change .server-mode select': 'onServerMode',
        'change .dh-param-bits select': 'onDhParamBits',
        'click .otp-auth-toggle': 'onOtpAuthSelect',
        'click .inter-client-toggle': 'onInterClientSelect',
        'click .debug-toggle': 'onDebugSelect',
        'click .multi-device-toggle': 'onMultiDeviceSelect'
      }, ModalServerSettingsView.__super__.events);
    },
    initialize: function(options) {
      this.localNetworks = options.localNetworks;
      ModalServerSettingsView.__super__.initialize.call(this);
    },
    body: function() {
      return this.template(this.model.toJSON());
    },
    postRender: function() {
      if (this.model.get('mode') === 'local_traffic') {
        this.$('.otp-auth-toggle').appendTo('.left');
        this.$('.otp-auth-toggle').slice(1).remove();
      }
      this.$('.local-network input').select2({
        tags: this.localNetworks,
        tokenSeparators: [',', ' '],
        placeholder: 'Select Local Networks',
        formatNoMatches: function() {
          return 'Enter Network Address';
        },
        width: '200px'
      });
      this.$('.local-network input').select2(
        'val', this.model.get('local_networks'));
      this.$('.network .label').tooltip();
      this.updateMaxHosts();
    },
    getServerMode: function() {
      return this.$('.server-mode select').val();
    },
    setServerMode: function(mode) {
      if (mode === 'local_traffic') {
        if (!this.$('.otp-auth-toggle').parent().hasClass('left')) {
          this.$('.local-network').slideDown(window.slideTime);
          this.$('.otp-auth-toggle').slideUp(window.slideTime, function() {
            this.$('.otp-auth-toggle').appendTo('.left:not(.advanced)');
            this.$('.otp-auth-toggle').slice(1).remove();
            this.$('.otp-auth-toggle').show();
          }.bind(this));
        }
      }
      else {
        if (!this.$('.otp-auth-toggle').parent().hasClass('right')) {
          this.$('.local-network').slideUp(window.slideTime);
          this.$('.otp-auth-toggle').slideUp(window.slideTime, function() {
            this.$('.otp-auth-toggle').appendTo('.right:not(.advanced)');
            this.$('.otp-auth-toggle').slice(1).remove();
            this.$('.otp-auth-toggle').show();
          }.bind(this));
        }
      }
    },
    onServerMode: function() {
      this.setServerMode(this.getServerMode());
    },
    onDhParamBits: function(evt) {
      var val = $(evt.target).val();
      if (val > 2048) {
        this.setAlert('danger', 'Using dh parameters larger then 2048 can ' +
          'take several hours to generate.', '.dh-param-bits');
      }
      else if (val > 1536) {
        this.setAlert('warning', 'Using dh parameters larger then 1536 can ' +
          'take several minutes to generate.', '.dh-param-bits');
      }
      else {
        this.clearAlert();
      }
    },
    getOtpAuthSelect: function() {
      return this.$('.otp-auth-toggle .selector').hasClass('selected');
    },
    setOtpAuthSelect: function(state) {
      if (state) {
        this.$('.otp-auth-toggle .selector').addClass('selected');
        this.$('.otp-auth-toggle .selector-inner').show();
      }
      else {
        this.$('.otp-auth-toggle .selector').removeClass('selected');
        this.$('.otp-auth-toggle .selector-inner').hide();
      }
    },
    onOtpAuthSelect: function() {
      this.setOtpAuthSelect(!this.getOtpAuthSelect());
    },
    getInterClientSelect: function() {
      return this.$('.inter-client-toggle .selector').hasClass('selected');
    },
    setInterClientSelect: function(state) {
      if (state) {
        this.$('.inter-client-toggle .selector').addClass('selected');
        this.$('.inter-client-toggle .selector-inner').show();
      }
      else {
        this.$('.inter-client-toggle .selector').removeClass('selected');
        this.$('.inter-client-toggle .selector-inner').hide();
      }
    },
    onInterClientSelect: function() {
      this.setInterClientSelect(!this.getInterClientSelect());
    },
    getDebugSelect: function() {
      return this.$('.debug-toggle .selector').hasClass('selected');
    },
    setDebugSelect: function(state) {
      if (state) {
        this.$('.debug-toggle .selector').addClass('selected');
        this.$('.debug-toggle .selector-inner').show();
      }
      else {
        this.$('.debug-toggle .selector').removeClass('selected');
        this.$('.debug-toggle .selector-inner').hide();
      }
    },
    onDebugSelect: function() {
      this.setDebugSelect(!this.getDebugSelect());
    },
    getMultiDeviceSelect: function() {
      return this.$('.multi-device-toggle .selector').hasClass('selected');
    },
    setMultiDeviceSelect: function(state) {
      if (state) {
        this.$('.multi-device-toggle .selector').addClass('selected');
        this.$('.multi-device-toggle .selector-inner').show();
      }
      else {
        this.$('.multi-device-toggle .selector').removeClass('selected');
        this.$('.multi-device-toggle .selector-inner').hide();
      }
    },
    onMultiDeviceSelect: function() {
      this.setMultiDeviceSelect(!this.getMultiDeviceSelect());
    },
    onInputChange: function(evt) {
      if ($(evt.target).parent().hasClass('network')) {
        this.updateMaxHosts();
      }
    },
    updateMaxHosts: function() {
      var value = this.$('.network input').val().split('/');
      var maxHosts = {
        8: '16m',
        9: '8m',
        10: '4m',
        11: '2m',
        12: '1m',
        13: '524k',
        14: '262k',
        15: '131k',
        16: '65k',
        17: '32k',
        18: '16k',
        19: '8k',
        20: '4k',
        21: '2k',
        22: '1k',
        23: '509',
        24: '253',
        25: '125',
        26: '61',
        27: '29',
        28: '13',
        29: '5',
        30: '1'
      };
      if (value.length === 2) {
        var max = maxHosts[value[1]];
        if (max) {
          this.$('.network .label').text(max + ' Users');
          this.$('.network .label').show();
          return;
        }
      }
      this.$('.network .label').hide();
    },
    onOk: function() {
      var i;
      var name = this.$('.name input').val();
      var network = this.$('.network input').val();
      var port = this.$('input.port').val();
      var protocol = this.$('select.protocol').val();
      var dhParamBits = parseInt(this.$('.dh-param-bits select').val(), 10);
      var mode = this.$('.server-mode select').val();
      var multiDevice = this.getMultiDeviceSelect();
      var dnsServers = [];
      var dnsServersTemp = this.$('.dns-servers input').val().split(',');
      for (i = 0; i < dnsServersTemp.length; i++) {
        dnsServersTemp[i] = $.trim(dnsServersTemp[i]);
        if (dnsServersTemp[i]) {
          dnsServers.push(dnsServersTemp[i]);
        }
      }
      var searchDomain = this.$('.search-domain input').val();
      var localNetworks = [];
      var interClient = this.getInterClientSelect();
      var pingInterval = parseInt(this.$('.ping-interval input').val(), 10);
      var pingTimeout = parseInt(this.$('.ping-timeout input').val(), 10);
      var maxClients = parseInt(this.$('.max-clients input').val(), 10);
      var replicaCount = parseInt(this.$('.replica-count input').val(), 10);
      var debug = this.getDebugSelect();
      var otpAuth = this.getOtpAuthSelect();
      var cipher = this.$('.cipher select').val();
      var bindAddress = this.$('.bind-address input').val();
      if (!bindAddress) {
        bindAddress = null;
      }

      if (!name) {
        this.setAlert('danger', 'Name can not be empty.', '.name');
        return;
      }
      if (!network) {
        this.setAlert('danger', 'Network can not be empty.', '.network');
        return;
      }
      if (!port) {
        this.setAlert('danger', 'Port can not be empty.', 'input.port');
        return;
      }
      if (this.getServerMode() === 'local_traffic') {
        localNetworks = this.$('.local-network input').select2('val');
        if (!localNetworks) {
          this.setAlert('danger', 'Local network can not be empty.',
            '.local-network');
          return;
        }
      }
      if (!searchDomain) {
        searchDomain = null;
      }
      if (isNaN(replicaCount) || replicaCount === 0) {
        replicaCount = 1;
      }

      var data = {
        'name': name,
        'type': this.model.get('type'),
        'network': network,
        'bind_address': bindAddress,
        'port': port,
        'protocol': protocol,
        'dh_param_bits': dhParamBits,
        'mode': mode,
        'multi_device': multiDevice,
        'local_networks': localNetworks,
        'dns_servers': dnsServers,
        'search_domain': searchDomain,
        'otp_auth': otpAuth,
        'cipher': cipher,
        'inter_client': interClient,
        'ping_interval': pingInterval,
        'ping_timeout': pingTimeout,
        'max_clients': maxClients,
        'replica_count': replicaCount,
        'debug': debug
      };

      this.setLoading(this.loadingMsg);
      this.model.save(data, {
        success: function() {
          this.close(true);
        }.bind(this),
        error: function(model, response) {
          this.clearLoading();
          if (response.responseJSON) {
            this.setAlert('danger', response.responseJSON.error_msg);
          }
          else {
            this.setAlert('danger', this.errorMsg);
          }
        }.bind(this)
      });
    }
  });

  return ModalServerSettingsView;
});
