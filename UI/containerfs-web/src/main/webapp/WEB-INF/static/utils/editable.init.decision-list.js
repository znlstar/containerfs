/**
 * Author: wangjunfu
 * Component: Editable
 * Date: 2017-03-10
 */

(function ($) {

    'use strict';

    var EditableTable = {

        options: {
            jsonButton: '#getJsonTable',
            addButton: '#searchList',
            table: '#datatable-editable'
        },

        initialize: function () {
            this
                .setVars()
                .build()
                .events();
        },

        setVars: function () {
            this.$table = $(this.options.table);
            this.$addButton = $(this.options.addButton);
            this.$jsonButton = $(this.options.jsonButton);

            return this;
        },

        build: function () {
            this.datatable = this.$table.DataTable({
                aoColumns: [
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    {"bSortable": false}
                ]
            });

            window.dt = this.datatable;

            return this;
        },

        events: function () {
            var _self = this;

            this.$table.on('click', 'a.remove-row', function (e) {
                e.preventDefault();

                // 删除当前行
                var $row = $(this).closest('tr');
                _self.rowRemove($row);

                // 触发ajax-json事件
                //_self.ajaxJson();
            });

            this.$addButton.on('click', function (e) {
                e.preventDefault();

                _self.rowAdd();
            });

            this.$jsonButton.on('click', function (e) {
                e.preventDefault();

                _self.ajaxJson();
            })

            return this;
        },

        // ==========================================================================================
        // ROW FUNCTIONS
        // ==========================================================================================
        rowAdd: function () {

            var _self = this,
                actions,
                data,
                $row;

            actions = [
                '<a href="javascript:void(0)" class="on-default remove-row"><i class="fa fa-trash-o"></i></a>'
            ].join(' ');

            // 这里要给新增一行赋值
            // ajax 异步获取因子信息
            var decisionKey = $('#decisionKey').val();
            var decisionId = $('#decisionId').val();

            $.ajax({
                url: "/business/getDecisionInfo",
                type: "POST",
                data: {decisionKey: decisionKey, decisionId: decisionId},
                async: true,
                error: function (request) {
                    // 提交失败
                    showInfoAlert("服务器连接失败，请检查网络环境！", null);
                },
                success: function (success) {
                    var r = eval("(" + success + ")");

                    if (r.returnCode != "1") {
                        showInfoAlert(decodeURI(r.returnMsg), null);
                    } else {
                        // 多条记录循环添加
                        var json = eval("(" + r.returnResult + ")");

                        $.each(json, function (idx, obj) {

                            // 追加数据
                            data = _self.datatable.row.add([obj.isUse, obj.decisionId, decodeURI(obj.decisionName), obj.decisionKey, obj.version, obj.returnType, actions]);
                            $row = _self.datatable.row(data[0]).nodes().to$();

                            $row
                                .find('td:last')
                                .addClass('actions');

                            _self.rowEdit($row);
                            _self.datatable.order([0, 'asc']).draw(); // 排序
                        });

                        // 触发ajax-json事件
                        //_self.ajaxJson();
                    }
                }
            });

        },

        rowEdit: function ($row) {
            var _self = this, data, len;
            var decision_names = ["isUse", "f_decisionId", "f_decisionName", "f_decisionKey", "f_version", "f_returnType", "action"];    //字段段名 by wangjufnu

            data = this.datatable.row($row.get(0)).data();

            $row.children('td').each(function (i) {
                var $this = $(this);

                if (i == 0 || i == 1) len = 50; else len = 150;

                if ($this.hasClass('actions')) {
                    // 什么也不做
                } else {
                    $this.html("<input type='text' id='" + decision_names[i] + "' name='" + decision_names[i] + "' class='form-control input-block' " +
                        "value='" + data[i] + "' readonly style='height: 28px; padding: 2px 10px; width: " + len + "px;' />");
                }

                if (i == 0) {
                    $this.css("display", "none");
                }
            });
        },

        rowRemove: function ($row) {
            this.datatable.row($row.get(0)).remove().draw();
        },

        // 触发ajax-json事件
        ajaxJson: function () {
            var decisionKeys = "";

            this.$table.find("input[name='f_decisionKey']").each(function () {
                decisionKeys += $(this).val() + ',';
            });

            if (decisionKeys == null || decisionKeys == "") {
                return false;
            }

            var decisionIds = "";

            this.$table.find("input[name='f_decisionId']").each(function () {
                decisionIds += $(this).val() + ',';
            });

            $.ajax({
                url: "/business/autoGetDecisionkeyJson",
                type: "POST",
                data: {decisionKeys: decisionKeys, decisionIds: decisionIds},
                async: false,
                success: function (data) {
                    var r = eval("(" + data + ")");
                    if (r.returnCode == "1") {
                        $('#businessJson').val(r.businessJson);
                    }
                }
            });
        }

    };

    $(function () {
        EditableTable.initialize();
    });

}).apply(this, [jQuery]);