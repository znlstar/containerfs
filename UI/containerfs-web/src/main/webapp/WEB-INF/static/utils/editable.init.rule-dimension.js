/**
 * Author: wangjunfu
 * Component: Editable
 * Date: 2016-12-22
 */

(function ($) {

    'use strict';

    var EditableTable = {

        options: {
            addButton: '#addToTable',
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

            return this;
        },

        build: function () {
            this.datatable = this.$table.DataTable({
                aoColumns: [
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

            this.$table
                .on('click', 'a.save-row', function (e) {
                    e.preventDefault();

                    _self.rowSave($(this).closest('tr'));
                })
                .on('click', 'a.cancel-row', function (e) {
                    e.preventDefault();

                    _self.rowCancel($(this).closest('tr'));
                })
                .on('click', 'a.edit-row', function (e) {
                    e.preventDefault();

                    _self.rowEdit($(this).closest('tr'));
                })
                .on('click', 'a.remove-row', function (e) {
                    e.preventDefault();

                    var $row = $(this).closest('tr');
                    var data = _self.datatable.row($row.get(0)).data();

                    /* artdialog start */
                    var d = dialog({
                        width: 320,
                        height: 'auto',
                        title: '提示',
                        content: '你确定要删除所选记录吗?',
                        fixed: true,
                        okValue: '确定',
                        ok: function () {
                            // ajax 异步删除 by wangjunfu
                            $.ajax({
                                url: "/ruledimension/deleteInfo",
                                type: "POST",
                                data: {dimId: data[0]},
                                async: false,
                                error: function (request) {
                                    // 提交失败
                                    showInfoAlert("服务器连接失败，请检查网络环境！", null);
                                },
                                success: function (data) {
                                    var r = eval("(" + data + ")");

                                    showInfoAlert(decodeURI(r.returnMsg), null);

                                    if (r.returnCode == "1") {
                                        // 删除当前行
                                        _self.rowRemove($row);
                                    }
                                }
                            });
                        },
                        cancelValue: '取消',
                        cancel: function () {
                        }
                    });

                    d.showModal();
                    /* artdialog end */
                });

            this.$addButton.on('click', function (e) {
                e.preventDefault();

                _self.rowAdd();
            });

            return this;
        },

        // ==========================================================================================
        // ROW FUNCTIONS
        // ==========================================================================================
        rowAdd: function () {
            this.$addButton.attr({'disabled': 'disabled'});

            var actions,
                data,
                $row;

            actions = [
                '<a href="javascript:void(0)" class="hidden on-editing save-row"><i class="fa fa-save"></i></a>',
                '<a href="javascript:void(0)" class="hidden on-editing cancel-row"><i class="fa fa-times"></i></a>',
                '<a href="javascript:void(0)" class="on-default edit-row"><i class="fa fa-pencil"></i></a>',
                '<a href="javascript:void(0)" class="on-default remove-row"><i class="fa fa-trash-o"></i></a>'
            ].join(' ');

            data = this.datatable.row.add(['', '', '', '', actions]);
            $row = this.datatable.row(data[0]).nodes().to$();

            $row
                .addClass('adding')
                .find('td:last')
                .addClass('actions');

            this.rowEdit($row);

            this.datatable.order([0, 'asc']).draw(); // always show fields
        },

        rowCancel: function ($row) {
            var _self = this,
                $actions,
                i,
                data;

            if ($row.hasClass('adding')) {
                this.rowRemove($row);
            } else {

                data = this.datatable.row($row.get(0)).data();
                this.datatable.row($row.get(0)).data(data);

                $actions = $row.find('td.actions');
                if ($actions.get(0)) {
                    this.rowSetActionsDefault($row);
                }

                this.datatable.draw();
            }
        },

        rowEdit: function ($row) {
            var _self = this, data;
            var dimension_names = ["dimId", "dimName", "dimCode", "sort", "action"];    //字段段名 by wangjufnu

            data = this.datatable.row($row.get(0)).data();

            $row.children('td').each(function (i) {
                var $this = $(this);

                if ($this.hasClass('actions')) {
                    _self.rowSetActionsEditing($row);
                } else {
                    if (i == 0) {
                        // id 不可修改
                        $this.html('<input type="text" disabled="disabled" name="' + dimension_names[i] + '" class="form-control input-block" value="' + data[i] + '" />');
                    } else {
                        $this.html('<input type="text" name="' + dimension_names[i] + '" class="form-control input-block" value="' + data[i] + '" />');
                    }
                }
            });
        },

        rowSave: function ($row) {
            var _self = this,
                $actions,
                values = [];

            // Add 按钮恢复原状，如果是编辑则无需修改。
            if ($row.hasClass('adding')) {
                this.$addButton.removeAttr('disabled');
                $row.removeClass('adding');
            }

            values = $row.find('td').map(function () {
                var $this = $(this);

                if ($this.hasClass('actions')) {
                    return _self.datatable.cell(this).data();
                } else {
                    return $.trim($this.find('input').val());
                }
            });

            // ajax 异步保存到数据库 by wangjunfu
            $.ajax({
                url: "/ruledimension/saveEdit",
                type: "POST",
                data: {
                    dimId: values[0],
                    dimName: values[1],
                    dimCode: values[2],
                    sort: values[3]
                },
                async: false,
                error: function (request) {
                    // 提交失败
                    showInfoAlert("服务器连接失败，请检查网络环境！", null);
                },
                success: function (data) {
                    var r = eval("(" + data + ")");

                    showInfoAlert(decodeURI(r.returnMsg), null);

                    if (r.returnCode == "1") {
                        // 提交成功 刷新列表
                        values[0] = r.dimId;
                        _self.datatable.row($row.get(0)).data(values);

                        $actions = $row.find('td.actions');
                        if ($actions.get(0)) {
                            _self.rowSetActionsDefault($row);
                        }

                        _self.datatable.draw();

                        // Action 按钮恢复原状
                        _self.rowSetActionsDefault($row);
                    }
                }
            });

        },

        rowRemove: function ($row) {
            if ($row.hasClass('adding')) {
                this.$addButton.removeAttr('disabled');
            }

            this.datatable.row($row.get(0)).remove().draw();
        },

        // Action 按钮
        rowSetActionsEditing: function ($row) {
            $row.find('.on-editing').removeClass('hidden');
            $row.find('.on-default').addClass('hidden');
        },

        // Action 按钮
        rowSetActionsDefault: function ($row) {
            $row.find('.on-editing').addClass('hidden');
            $row.find('.on-default').removeClass('hidden');
        }

    };

    $(function () {
        EditableTable.initialize();
    });

}).apply(this, [jQuery]);