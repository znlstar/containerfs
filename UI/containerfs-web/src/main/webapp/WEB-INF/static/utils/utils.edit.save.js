var _controller;

function editSaveInit(controller) {
    this._controller = controller;

    //新增 修改
    $('#saveform').click(function () {
        //表单验证
        if (!beforeSubmit()) {
            return false;
        }
        showInfoAlertSelect("你确定要提交当前记录吗?", saveFunc);
    });
}

function saveFunc() {
    $.ajax({
        url: "/" + this._controller + "/saveEdit",
        type: "POST",
        data: $('#myform').serializeArray(),
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
