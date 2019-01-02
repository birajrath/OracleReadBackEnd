<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<jsp:include page="../cdg_header.jsp" />
<script>
	function jsonconstruct(val) {
		var data = {};
		document.getElementById('button_type').value = val;
		$(".form-control").serializeArray().map(function(x) {
			data[x.name] = x.value;
		});
		var x = '{"header":{"user":"info@clouddatagrid.com","service_account":"Extraction_CDG_UK","reservoir_id":"R0001","event_time":"today"},"body":{"data":'
				+ JSON.stringify(data) + '}}';
		document.getElementById('x').value = x;
		//console.log(x);
		//alert(x);
		document.getElementById('ConnectionDetails').submit();
	}
	$(document).ready(function() {
		$("#conn").change(function() {
		var conn = $(this).val();
		var src_val = document.getElementById("src_val").value;
		$.post('${pageContext.request.contextPath}/extraction/ConnectionDetailsEdit',
		{
			conn : conn,
			src_val : src_val
		}, function(data) {
			$('#cud').html(data)
		});
	});
});

	function funccheck(val) {
		if (val == 'create') {
			window.location.reload();
		} else {
			document.getElementById('connfunc').style.display = "block";
			document.getElementById('connfunc').innerHTML = "";
		}
	}
</script>
<div class="main-panel">
	<div class="content-wrapper">
		<div class="row">
			<div class="col-12 grid-margin stretch-card">
				<div class="card">
					<div class="card-body">
						<h4 class="card-title">Data Extraction</h4>
						<p class="card-description">Connection Details</p>
						<%
							if (request.getAttribute("successString") != null) {
						%>
						<p class="text-success h4">${successString}</p>
						<%
							}
						%>
						<%
							if (request.getAttribute("errorString") != null) {
						%>
						<p class="text-danger h4">${errorString}</p>
						<%
							}
						%>
						<form class="forms-sample" id="ConnectionDetails"
							name="ConnectionDetails" method="POST"
							action="${pageContext.request.contextPath}/extraction/ConnectionDetails1"
							enctype="application/json">
							<input type="hidden" name="x" id="x" value=""> <input
								type="hidden" name="button_type" id="button_type" value="">
							<input type="hidden" name="src_val" id="src_val"
								value="${src_val}">
							<input type="hidden" name="path" id="path" class="form-control"
								value="">
								<input type="hidden" name="project" id="project" class="form-control"
								value="${project}">
								<input type="hidden" name="user" id="user" class="form-control"
								value="${usernm}">

							<div class="form-group row">
								<label class="col-sm-3 col-form-label">Connection</label>
								<div class="col-sm-4">
									<div class="form-check form-check-info">
										<label class="form-check-label"> <input type="radio"
											class="form-check-input" name="radio" id="radio1"
											checked="checked" value="create"
											onclick="funccheck(this.value)"> Create
										</label>
									</div>
								</div>
								<div class="col-sm-4">
									<div class="form-check form-check-info">
										<label class="form-check-label"> <input type="radio"
											class="form-check-input" name="radio" id="radio2"
											value="edit" onclick="funccheck(this.value)"> Edit
										</label>
									</div>
								</div>
							</div>

							<div class="form-group" id="connfunc" style="display: none;">
								<label>Select Connection</label> <select name="conn" id="conn"
									class="form-control">
									<option value="" selected disabled>Select Connection
										...</option>
									<c:forEach items="${conn_val}" var="conn_val">
										<option value="${conn_val.connection_id}">${conn_val.connection_name}</option>
									</c:forEach>
								</select>
							</div>
							<div id="cud">
								<fieldset class="fs">
									<div class="form-group row">
										<div class="col-sm-6">
											<label>Connection Name *</label> <input type="text"
												class="form-control" id="connection_name"
												name="connection_name" placeholder="Connection Name">
										</div>
										<div class="col-sm-6">
											<label>Connection Type *</label> <input type="text"
												class="form-control" id="connection_type"
												name="connection_type" value="${src_val}"
												readonly="readonly">
										</div>
									</div>
									<div class="form-group row">
										<div class="col-sm-6">
										<label>Disk Drive *</label> <select name="drive_id" id="drive_id"
									class="form-control">
									<option value="" selected disabled>Select Disk Drive
										...</option>
									<c:forEach items="${drive}" var="drive">
										<option value="${drive.drive_id}">${drive.drive_name} - ${drive.drive_path}</option>
									</c:forEach>
								</select>
										</div>
										<div class="col-sm-6">
										<label>Select System *</label> 
									<select name="system" id="system" class="form-control">
										<option value="" selected disabled>Select System...</option>
											<c:forEach items="${system}" var="system">
												<option value="${system}">${system}</option>
											</c:forEach>
									</select>
										</div>
									</div>
								</fieldset>
								<button onclick="jsonconstruct('add');"
									class="btn btn-rounded btn-gradient-info mr-2">Test &
									Save</button>
							</div>
						</form>
					</div>
				</div>
			</div>
		</div>
		<jsp:include page="../cdg_footer.jsp" />