<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>

<div class="form-group row">
	<div class="col-sm-6">
		<label>Disk Drive *</label> 
		<c:forEach items="${drive}" var="drive">
		<input type="text" class="form-control" readonly="readonly"
			id="drive_id" name="drive_id" placeholder="Disk Drive" value="${drive.drive_name} - ${drive.drive_path}">
			</c:forEach>
	</div>
	<div class="col-sm-6">
		<label>Data Path *</label> <input type="text" class="form-control"
			id="data_path" name="data_path" placeholder="Data Path" value="">
	</div>
</div>