$(function () {
    //给全选/取消全绑定事件
    $('#checkbox-all').click(function () {
        checkbox_all(this.id, 'checkbox-select');

        //设置Edit和Delete按钮的状态
        if ($('#checkbox-all').is(':checked')) {
            $('#demo-edit-row').attr('disabled', "true");
            $('#demo-delete-row').removeAttr("disabled");
        }
        else {
            $('#demo-edit-row').attr('disabled', "true");
            $('#demo-delete-row').attr('disabled', "true");
        }
    });

    //给复选框cccav绑定事件
    $('input[name="cccav"]').click(function () {
        //判断edit按钮是否可用
        var len = $("input[type='checkbox']:checked").length;

        if (len == 1) {
            $('#demo-edit-row').removeAttr("disabled");
        } else {
            $('#demo-edit-row').attr('disabled', "true");
        }

        //判断delete按钮是否可用
        if (len > 0) {
            $('#demo-delete-row').removeAttr("disabled");
        } else {
            $('#demo-delete-row').attr('disabled', "true");
        }
    });

});