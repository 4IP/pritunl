define([
  'jquery',
  'underscore',
  'backbone'
], function($, _, Backbone) {
  'use strict';
  var VpcsModel = Backbone.Model.extend({
    defaults: {
      'us-east-1': [],
      'us-west-1': [],
      'us-west-2': [],
      'eu-west-1': [],
      'eu-central-1': [],
      'ap-northeast-1': [],
      'ap-northeast-2': [],
      'ap-southeast-1': [],
      'ap-southeast-2': [],
      'sa-east-1': []
    },
    url: '/server/vpcs'
  });

  return VpcsModel;
});
