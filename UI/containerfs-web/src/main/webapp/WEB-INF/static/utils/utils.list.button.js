var _controller;

function listButtonInit(controller) {
    this._controller = controller;

    //给Add按钮绑定事件
    $('#demo-add-row').click(function () {
        jumpTo("/" + controller + "/edit");
    });

    //给Edit按钮绑定事件
    $('#demo-edit-row').click(function () {
        var len = 0, id;

        $("input[type='checkbox']:checked").each(function () {
            len++;
            id = $(this).val();
        });

        if (len != 1) {
            showInfoAlert("请选择一条记录进行编辑！", null);
            return false;
        }

        //编辑操作
        jumpTo("/" + controller + "/edit?Id=" + id);
    });

    //给Delete按钮绑定事件
    $('#demo-delete-row').click(function () {
        showInfoAlertSelect("你确定要删除所选记录吗?", deleteFunc);
    });
}

function deleteFunc() {
    var len = 0;
    var ids = "";
    $("input[type='checkbox']:checked").each(function () {
        len++;
        ids += $(this).val() + ",";
    });

    if (len == 0) {
        showInfoAlert("请至少选择一条记录进行删除！", null);
        return false;
    }

    //删除操作
    $.ajax({
        url: "/" + this._controller + "/deleteInfo",
        type: "POST",
        data: {ids: ids},
        async: false,
        error: function (request) {
            //提交失败
            showInfoAlert("服务器连接失败，请检查网络环境！", null);
        },
        success: function (data) {
            var r = eval("(" + data + ")");
            if (r.returnCode == "1") {
                showInfoAlert(decodeURI(r.returnMsg), jumpToIndex);
            }
            else {
                showInfoAlert(decodeURI(r.returnMsg), null);
            }
        }
    });
}

//跳转
function jumpToIndex() {
    window.location = "/" + this._controller + "/index";
}
