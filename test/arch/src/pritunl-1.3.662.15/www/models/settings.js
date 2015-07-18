define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {
  'use strict';
  var SettingsModel = Backbone.Model.extend({
    defaults: {
      'username': null,
      'password': null,
      'token': null,
      'secret': null,
      'default': null,
      'email_server': null,
      'email_username': null,
      'email_password': null,
      'email_from': null,
      'sso': null,
      'sso_match': null,
      'sso_org': null,
      'public_address': null,
      'theme': null
    },
    url: function() {
      return '/settings';
    },
    isNew: function() {
      return false;
    }
  });

  return SettingsModel;
});
