/* 以下内容为自定义函数 */

//返回按钮上一级
function gotoBack() {
    javascript: history.go(-1);
    return;
}

//全选/取消全
function checkbox_all(id, checkboxID) {
    if (document.getElementById(id).checked) {
        $('#' + checkboxID + ' input').each(function () {
            $(this).attr('checked', true);
            $(this).parent().parent("tr").find("td").addClass("ins_mouse_color");
        });
    }
    else {
        $('#' + checkboxID + ' input').each(function () {
            $(this).attr('checked', false);
            $(this).parent().parent("tr").find("td").removeClass("ins_mouse_color");
        });
    }
}

//将小写字符转化成大写字符
function toUpCase(x) {
    var y = document.getElementById(x).value;
    document.getElementById(x).value = y.toUpperCase();
}

//跳转
function jumpTo(url) {
    window.location = url;
}

//鼠标经过table行颜色改变
$(function () {
    $("#checkbox-select tr").mouseover(function () {
        //$(this).find("td").addClass("ins_mouse_color");
    });

    $("#checkbox-select tr").mouseout(function () {
        //如果被选中了就不取消了
        if ($(this).find("input[type='checkbox']").is(':checked') == false) {
            $(this).find("td").removeClass("ins_mouse_color");
        }
    });
});