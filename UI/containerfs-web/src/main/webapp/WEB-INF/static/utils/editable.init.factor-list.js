/**
 * Author: wangjunfu
 * Component: Editable
 * Date: 2016-12-22
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
            var cateId = $('#cateId option:selected').val();
            var factorColumn = $('#factorColumn').val();

            $.ajax({
                url: "/rule/getFactorInfo",
                type: "POST",
                data: {cateId: cateId, factorColumn: factorColumn},
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
                            data = _self.datatable.row.add([obj.cateId, obj.factorId, decodeURI(obj.factorName), obj.factorColumn, obj.getMethod, obj.returnType, actions]);
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
            var factor_names = ["f_cateId", "f_factorId", "f_factorName", "f_factorColumn", "f_getMethod", "f_returnType", "action"];    //字段段名 by wangjufnu

            data = this.datatable.row($row.get(0)).data();

            $row.children('td').each(function (i) {
                var $this = $(this);

                if (i == 0 || i == 1 || i == 5) len = 50; else len = 150;

                if ($this.hasClass('actions')) {
                    // 什么也不做
                } else {
                    $this.html("<input type='text' id='" + factor_names[i] + "' name='" + factor_names[i] + "' class='form-control input-block' " +
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
            var cIds = "", fIds = "";

            this.$table.find("input[name='f_cateId']").each(function () {
                cIds += $(this).val() + ',';
            });

            this.$table.find("input[name='f_factorId']").each(function () {
                fIds += $(this).val() + ',';
            });

            if (cIds == null || cIds == "" || fIds == null || fIds == "") {
                return false;
            }

            $.ajax({
                url: "/rule/autoGetRowkeyJson",
                type: "POST",
                data: {cateIds: cIds, factorIds: fIds},
                async: false,
                success: function (data) {
                    var r = eval("(" + data + ")");
                    if (r.returnCode == "1") {
                        $('#rowkeyJson').val(r.rowkeyJson);
                        $('#ruleJson').val(r.ruleJson);
                    }
                }
            });
        }

    };

    $(function () {
        EditableTable.initialize();
    });

}).apply(this, [jQuery]);